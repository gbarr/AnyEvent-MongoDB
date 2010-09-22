## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Connection;

use Moose;
use AnyEvent::Handle;
use AnyEvent::Util qw(guard);
use Scalar::Util qw(weaken);
use MongoDB;
use MongoDB::Cursor;
use Digest::MD5 qw(md5_hex);

use namespace::autoclean;

has mongo => (
  weak_ref => 1,
  required => 1,
  handles  => [qw(w wtimeout timeout query_timeout)],
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
  clearer   => 'clear_handle',
  trigger   => sub {
    my ($self, $handle) = @_;

    if ($handle) {
      $self->clear_waiting;
      $self->flush_pending;
    }
  },
);

after clear_handle => sub {
  my $self = shift;
  $self->clear_pending;
  $self->clear_request_info;
  $self->clear_connected;
};


has connected => (
  is      => 'ro',
  isa     => 'Bool',
  traits  => ['Bool'],
  default => 0,
  handles => {
    clear_connected => 'unset',
    set_connected   => 'set',
  },
);

has waiting => (
  is      => 'ro',
  isa     => 'Bool',
  traits  => ['Bool'],
  default => 1,
  handles => {
    set_waiting   => 'set',
    clear_waiting => 'unset',
  }
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
    shift_pending => 'shift',
    has_pending   => 'count',
  },
);


has request_info => (
  isa     => 'HashRef[HashRef]',
  traits  => ['Hash'],
  default => sub { {} },
  handles => {
    set_request_info    => 'set',
    get_request_info    => 'get',
    delete_request_info => 'delete',
    clear_request_info  => 'clear',
  },
);

sub connect {
  my $self = shift;

  $self->clear_connected;
  $self->clear_request_info;

  my $timeout_sec = $self->timeout / 1000;

  weaken(my $_self = $self);    # avoid loops
  my $handle = AnyEvent::Handle->new(
    connect    => [$self->host, $self->port],
    on_connect => sub {
      $_self->set_connected;
      $_self->clear_waiting;
      my $cb = $_self->on_connect;
      $cb->($_self) if $cb;
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

sub flush_pending {
  my $self = shift;

  while (!$self->waiting and my $args = $self->shift_pending) {
    $self->send_request(@$args);
  }
}

sub send_request {
  my ($self, $message, $request_id, $request_info) = @_;

  if ($request_info) {
    $self->set_request_info($request_id, $request_info);
    my $timeout = $request_info->{timeout} || 0;
    if ($timeout > 0) {
      weaken(my $_self = $self);    # avoid loops
      $request_info->{guard} = AE::timer(
        $timeout / 1000,
        0,
        sub {
          if (my $rinfo = $_self->get_request_info($request_id)) {
            delete $rinfo->{guard};
            $rinfo->{cancelled} = 1;
            if (my $cb = $rinfo->{cb}) {
              $cb->({'$err' => 'query timeout'});
            }
          }
        }
      );
    }

    $self->set_waiting if $request_info->{cb};
  }

  $self->handle->push_write($message);

  return;
}

sub push_request {
  my $self = shift;

  if ($self->has_pending or $self->waiting) {
    $self->push_pending(\@_);
  }
  else {
    $self->send_request(@_);
  }
  my $request_info = $_[2];

  return unless defined(wantarray) and $request_info;
  my $cancel = \($request_info->{cancelled});
  return guard { $$cancel = 1; };
}

sub op_get_more {
  my ($self, $request_id, $cursor_id, $request_info, $page) = @_;

  my $next_request_id = ++$MongoDB::Cursor::_request_id;
  my $message         = pack(
    "V V V V   V Z* V a8",
    0, $next_request_id,    $request_id, 2005,        # OP_GET_MORE
    0, $request_info->{ns}, $page,       $cursor_id
  );
  substr($message, 0, 4) = pack("V", length($message));
  $self->push_request($message, $next_request_id, $request_info);
}

sub op_kill_cursors {
  my ($self, $request_id, $cursor_id) = @_;
  my $next_request_id = ++$MongoDB::Cursor::_request_id;
  my $message         = pack(
    "V V V V   V V a8",
    0, $next_request_id, $request_id, 2007,    # OP_KILL_CURSORS
    0, 1,                $cursor_id
  );
  substr($message, 0, 4) = pack("V", length($message));
  $self->handle->push_write($message);
}

sub _callbacks {
  my $self = shift;

  weaken(my $_self = $self);    # avoid loops
  return (
    on_error => $self->on_error,
    on_read  => sub {
      shift->unshift_read(
        chunk => 36,
        sub {
          return unless $_self;

          # header arrived, decode
          # We extract cursor with Z8 so that it will work correctly on 32-bit machines
          # using Z means that when cursor_id is all zero it is extracted as ''
          # so length($cursor_id) can be used to determine if we got a cursor id
          my ($len, $request_id, $response_to, $op_code, $flags, $cursor_id, $from, $doc_count) =
            unpack "V V V V V Z8 V V", $_[1];

          my $blen = $len - 36;
          if ($blen < 0) {
            die;    # XXX FIXME
          }

          my $request_info = $_self->delete_request_info($response_to) || {};

          # cancel timeout timer
          delete $request_info->{guard};

          my $cb = $request_info->{cb}
            and $_self->clear_waiting;

          $_self->flush_pending;

          if ($blen) {

            # We have documents to read
            shift->unshift_read(
              chunk => $blen,
              sub {
                my $at = $request_info->{at} += $doc_count;
                my $max = $request_info->{max} || 0;
                my $done = $max && $at >= $max;
                my $do_prefetch = $request_info->{prefetch} && !$done && length($cursor_id);

                my $page = do {
                  my $todo = $max && $max - $at;
                  my $p = $request_info->{limit};
                  ($todo && $todo < $p) ? $todo : $p;
                };

                if ($request_info->{cancelled}) {

                  # The cursor was cancelled after we sent a prefetch OP_GET_MORE
                  # So we need to just ignore this OP_RESULT and cancel if needed

                  $_self->op_kill_cursors($request_id, $cursor_id)
                    if length($cursor_id);

                  return;
                }

                if ($do_prefetch) {

                  # User has requested prefetch, so lets send the OP_GET_MORE
                  # request before we start processing this one
                  $_self->op_get_more($request_id, $cursor_id, $request_info, $page);
                }

                my $want_more = $cb && $cb->(MongoDB::read_documents($_[1]));

                if ($want_more) {
                  if (length($cursor_id) and !$done) {
                    $_self->op_get_more($request_id, $cursor_id, $request_info, $page)
                      unless $do_prefetch;    # already sent
                  }
                  elsif ($page >= 0) {
                    $cb->() if $cb;                                       # signal finished
                  }
                }

                if (length($cursor_id) and ($done or !$want_more)) {
                  if ($do_prefetch) {

                    # We have already requested the next page of results,
                    # so will need to cancel when we get that
                    delete $request_info->{cb};
                    $request_info->{cancelled} = 1;
                  }
                  else {
                    $_self->op_kill_cursors($request_id, $cursor_id);
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
      $_self->clear_handle;
      my $cb = $_self->on_eof;
      $cb->() if $cb;
    },

  );
}


sub op_query {
  my ($self, $opt) = @_;

  my $flags = ($opt->{tailable} ? 1 << 1 : 0)    ##
    | ($opt->{slave_okay} ? 1 << 2 : 0)          ##
    | ($opt->{immortal}   ? 1 << 4 : 0);

  my $limit = $opt->{limit} || 0;
  my $page  = $opt->{page}  || 100;

  $page = $limit if $limit and $limit < $page;

  my ($query, $info) = MongoDB::write_query(
    $opt->{ns},    ##
    $flags,        ##
    $opt->{skip} || 0,
    $page,
    $opt->{query}  || {},
    $opt->{fields} || {}
  );

  # TODO: $opt->{all} - fetch all documents before calling cb, just once
  $info->{exhaust}  = $opt->{exhaust};                                  # TODO
  $info->{prefetch} = exists $opt->{prefetch} ? $opt->{prefetch} : 1;
  $info->{cb}       = $opt->{cb};
  $info->{max}      = abs($limit);
  $info->{timeout}  = $opt->{timeout} || $self->query_timeout;

  # MongoDB returns an alias to the scalar in $info hash,
  # which means it changes the next time $MongoDB::Cursor::_request_id
  # is incremented. but we want a copy
  $info->{request_id} = 0 + delete $info->{request_id};

  $self->push_request($query, $info->{request_id}, $info);
}

sub push_safe_request {
  my ($self, $message, $opt) = @_;
  my ($request_id, $info);

  if ($opt->{cb}) {
    my $last_error = Tie::IxHash->new(
      getlasterror => 1,
      w            => int($opt->{w} || $self->w),
      wtimeout     => int($opt->{wtimeout} || $self->wtimeout),
    );
    (my $db = $opt->{ns}) =~ s/\..*//;
    (my $query, $info) = MongoDB::write_query($db . '.$cmd', 0, 0, -1, $last_error);
    $info->{cb} = $opt->{cb};
    $info->{timeout} = $opt->{timeout} || $self->query_timeout;

    $request_id = $info->{request_id};
    $message .= $query;
  }
  $self->push_request($message, $request_id, $info);
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

  $self->push_safe_request($update, $opt);
}

sub op_insert {
  my ($self, $opt) = @_;

  my ($insert, $ids) = MongoDB::write_insert($opt->{ns}, $opt->{documents});

  $self->push_safe_request($insert, $opt);

  return $ids;
}

sub op_delete {
  my ($self, $opt) = @_;

  my ($delete) = MongoDB::write_remove($opt->{ns}, $opt->{query}, $opt->{just_one} || 0);

  $self->push_safe_request($delete, $opt);
}

sub authenticate {
  my ($self, $db, $user, $pass, $cb) = @_;

  weaken(my $_self = $self);    # avoid loops
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
        $_self->op_query(
          { ns    => $db . '$cmd',
            limit => -1,
            query => $auth,
            cb    => $cb,
          }
        );
      },
    }
  );
}


sub last_error {
  my ($self, $opt) = @_;

  if ($opt->{cb}) {
    if ($self->has_handle) {
      $self->push_safe_request('', $opt);
    }
    else {
      $opt->{cb}->({err => "no connection"});
    }
  }

  return;
}

__PACKAGE__->meta->make_immutable;
