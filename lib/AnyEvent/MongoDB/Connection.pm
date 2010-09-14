## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Connection;

use Moose;
use AnyEvent::Handle;
use Scalar::Util qw(weaken);
use MongoDB;
use MongoDB::Cursor;
use Digest::MD5 qw(md5_hex);

use namespace::autoclean;

has mongo => (
  weak_ref => 1,
  required => 1,
  handles  => [qw(w wtimeout timeout)],
);

has host => (
  is  => 'ro',
  isa => 'Str',
);

has is_master => (
  is      => 'ro',
  default => 1,
);

has port => (
  is  => 'ro',
  isa => 'Int',
);

has handle => (
  is        => 'rw',
  predicate => 'has_handle',
  trigger   => sub {
    my ($self, $handle) = @_;
    if ($handle) {

      # XXX what if pending are op_query with query timeout ?
      $handle->push_write($_) for $self->pending;
      $self->clear_pending;
    }
  },
);

has connected => (
  is      => 'rw',
  isa     => 'Bool',
  default => 0,
  clearer => 'clear_connected',
);

has [qw/ on_connect on_connect_error on_error on_read on_eof /] => (
  is  => 'rw',
  isa => 'CodeRef'
);

has pending => (
  traits  => ['Array'],
  default => sub { [] },
  handles => {
    pending       => 'elements',
    clear_pending => 'clear',
    push_pending  => 'push',
  },
);


has request_info => (
  isa     => 'HashRef[HashRef]',
  traits  => ['Hash'],
  default => sub { {} },
  handles => {
    set_request_info   => 'set',
    get_request_info   => 'delete',
    clear_request_info => 'clear',
  },
);

sub connect {
  my $self = shift;

  $self->clear_connected;
  $self->clear_request_info;

  my $timeout_sec = $self->timeout / 1000;

  weaken($self);    # avoid loops
  my $handle = AnyEvent::Handle->new(
    connect    => [$self->host, $self->port],
    on_connect => sub {
      $self->connected(1);
      my $cb = $self->on_connect;
      $cb->($self) if $cb;
    },
    on_prepare       => sub { return $timeout_sec },
    on_connect_error => $self->on_connect_error,
    $self->_callbacks,
  );

  $self->handle($handle);
  return $self;
}

sub set_fh {
  my ($self, $fh) = @_;

  my $handle = AnyEvent::Handle->new(
    fh => $fh,
    $self->_callbacks,
  );
  $self->handle($handle);

  return;
}

sub _callbacks {
  my $self = shift;
  weaken($self);
  return (
    on_error => $self->on_error,
    on_read  => sub {
      shift->unshift_read(
        chunk => 36,
        sub {
          return unless $self;

          # header arrived, decode
          # We extract cursor with Z8 so that it will work correctl on 32-bit machines
          # using Z mean that when cursor_id is all zero it is extracted as ''
          # so length($cursor_id) can be used to determine if we got a cursor id
          my ($len, $request_id, $response_to, $op_code, $flags, $cursor_id, $from, $doc_count) =
            unpack "V V V V V Z8 V V", $_[1];

          my $blen = $len - 36;
          if ($blen < 0) {
            die;    # XXX FIXME
          }

          my $request_info = $self->get_request_info($response_to) || {};
          my $cb = $request_info->{cb};

          if ($blen) {

            # We have documents to read
            shift->unshift_read(
              chunk => $blen,
              sub {
                my $at = $request_info->{at} += $doc_count;
                my $limit = $request_info->{limit} || 0;
                my $todo = $limit && $limit - $at;
                my $done = $limit && $at >= $limit;
                my $do_prefetch = $request_info->{prefetch} && !$done && length($cursor_id);

                if ($request_info->{cancelled}) {

                  # The cursor was cancelled after we sent a prefetch OP_GET_MORE
                  # So we need to just ignore this OP_RESULT and cancel if needed
                  if (length($cursor_id)) {
                    my $next_request_id = ++$MongoDB::Cursor::_request_id;
                    my $message         = pack(
                      "V V V V   V V a8",
                      0, $next_request_id, $request_id, 2007,    # OP_KILL_CURSORS
                      0, 1,                $cursor_id
                    );
                    substr($message, 0, 4) = pack("V", length($message));
                    $self->handle->push_write($message);
                    $self->set_request_info($next_request_id, $request_info);
                  }
                  return;
                }

                if ($do_prefetch) {

                  # User has requested prefetch, so lets send the OP_GET_MORE
                  # request before we start processing this one
                  my $next_request_id = ++$MongoDB::Cursor::_request_id;
                  my $message         = pack(
                    "V V V V   V Z* V a8",
                    0, $next_request_id,    $request_id, 2005,        # OP_GET_MORE
                    0, $request_info->{ns}, $todo,       $cursor_id
                  );
                  substr($message, 0, 4) = pack("V", length($message));
                  $self->handle->push_write($message);
                  $self->set_request_info($next_request_id, $request_info);
                }

                my $want_more = $cb && $cb->(MongoDB::read_documents($_[1]));

                if ($want_more) {
                  if (length($cursor_id) and !$done) {
                    unless ($do_prefetch) {
                      my $next_request_id = ++$MongoDB::Cursor::_request_id;
                      my $message         = pack(
                        "V V V V   V Z* V a8",
                        0, $next_request_id,    $request_id, 2005,        # OP_GET_MORE
                        0, $request_info->{ns}, $todo,       $cursor_id
                      );
                      substr($message, 0, 4) = pack("V", length($message));
                      $self->handle->push_write($message);
                      $self->set_request_info($next_request_id, $request_info);
                    }
                  }
                  elsif ($limit >= 0) {
                    $cb->() if $cb;                                       # signal finished
                  }
                }

                if (length($cursor_id) and ($done or !$want_more) and $limit >= 0) {
                  if ($do_prefetch) {

                    # We have already requested the next page of results,
                    # so will need to cancel when we get that
                    delete $request_info->{cb};
                    $request_info->{cancelled} = 1;
                  }
                  else {
                    my $next_request_id = ++$MongoDB::Cursor::_request_id;
                    my $message         = pack(
                      "V V V V   V V a8",
                      0, $next_request_id, $request_id, 2007,    # OP_KILL_CURSORS
                      0, 1,                $cursor_id
                    );
                    substr($message, 0, 4) = pack("V", length($message));
                    $self->handle->push_write($message);
                    $self->set_request_info($next_request_id, $request_info);
                  }
                }
              }
            );
          }
          elsif ($cb) {

            # No documents in OP_REPLY, just call callback to signal end of cursor
            $cb->();
          }
        }
      );
    },
    on_eof => sub {
      $self->connected(0);
      $self->clear_handle;
      my $cb = $self->on_eof;
      $cb->() if $cb;
    },

  );
}


sub op_query {
  my ($self, $opt) = @_;

  my $flags = ($opt->{tailable} ? 1 << 1 : 0)    ##
    | ($opt->{slave_okay} ? 1 << 2 : 0)          ##
    | ($opt->{immortal}   ? 1 << 4 : 0);

  my ($query, $info) = MongoDB::write_query(
    $opt->{ns}, $flags,
    $opt->{skip}   || 0,
    $opt->{limit}  || 0,
    $opt->{query}  || {},
    $opt->{fields} || {}
  );

  # TODO: $opt->{all} - fetch all documents before calling cb, just once
  # TODO: $opt->{batch_size}
  # TODO: $opt->{timeout}
  $info->{exhaust}  = $opt->{exhaust};                                  # TODO
  $info->{prefetch} = exists $opt->{prefetch} ? $opt->{prefetch} : 1;
  $info->{cb}       = $opt->{cb};

  $self->set_request_info($info->{request_id} => $info);

  if ($self->has_handle) {
    $self->handle->push_write($query);
  }
  else {
    $self->push_pending($query);
  }

  return;
}

sub _make_safe {
  my ($self, $opt) = @_;

  my $last_error = Tie::IxHash->new(
    getlasterror => 1,
    w            => $opt->{w} || $self->w,
    wtimeout     => $opt->{wtimeout} || $self->wtimeout
  );
  (my $db = $opt->{ns}) =~ s/\..*//;
  my ($query, $info) = MongoDB::write_query($db . '.$cmd', 0, 0, -1, $last_error);
  $info->{cb} = $opt->{cb};

  $self->set_request_info($info->{request_id} => $info);
  return $query;
}

sub op_update {
  my ($self, $opt) = @_;

  my $flags = ($opt->{upsert} ? 1 : 0) | ($opt->{multiple} ? 1 << 1 : 0);

  my ($update) = MongoDB::write_update(    ##
    $opt->{ns},
    $opt->{query}  || {},
    $opt->{update} || {},
    $flags,
  );

  $update .= $self->_make_safe($opt) if $opt->{cb};

  if ($self->has_handle) {
    $self->handle->push_write($update);
  }
  else {
    $self->push_pending($update);
  }

  return;
}

sub op_insert {
  my ($self, $opt) = @_;

  my ($insert, $ids) = MongoDB::write_insert($opt->{ns}, $opt->{documents});

  $insert .= $self->_make_safe($opt) if $opt->{cb};

  if ($self->has_handle) {
    $self->handle->push_write($insert);
  }
  else {
    $self->push_pending($insert);
  }

  return $ids;
}

sub op_delete {
  my ($self, $opt) = @_;

  my ($delete) = MongoDB::write_remove($opt->{ns}, $opt->{query}, $opt->{just_one} || 0);

  $delete .= $self->_make_safe($opt) if $opt->{cb};

  if ($self->has_handle) {
    $self->handle->push_write($delete);
  }
  else {
    $self->push_pending($delete);
  }

  return;
}

sub authenticate {
  my ($self, $db, $user, $pass, $cb) = @_;

  weaken($self);    # avoid loops
  $self->op_query(
    { ns    => $db . '$cmd',
      limit => -1,
      query => {getnonce => 1},
      cb    => sub {
        my $result = shift;
        unless ($result and $result->{ok}) {
          $cb->($result) if $cb;
          return;
        }

        my $hash   = md5_hex("${user}:mongo:${pass}");
        my $digest = md5_hex($result->{nonce} . $user . $hash);
        my $auth   = Tie::IxHash->new(
          authenticate => 1,
          user         => $user,
          nonce        => $result->{nonce},
          key          => $digest,
        );
        $self->op_query(
          { ns    => $db . '$cmd',
            limit => -1,
            query => $auth,
            cb    => $cb,
          }
        );
        }
    }
  );
}


sub last_error {
  my ($self, $opt) = @_;

  if ($opt->{cb}) {
    if ($self->has_handle) {
      my $query = $self->_make_safe($opt);

      $self->handle->push_write($query);
    }
    else {
      $opt->{cb}->({err => "no connection"});
    }
  }

  return;
}

__PACKAGE__->meta->make_immutable;
