## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Connection;

use Moose;
use AnyEvent::Handle;
use AnyEvent::Socket qw(tcp_connect);
use AnyEvent::Util qw(guard);
use Scalar::Util qw(weaken);
use MongoDB;
use MongoDB::Cursor;
use MongoDB::OID;
use DateTime;
use Digest::MD5 qw(md5_hex);

use namespace::autoclean;

has mongo => (
  weak_ref => 1,
  required => 1,
  handles  => [qw(timeout)],
);

has host => (
  is  => 'ro',
  isa => 'Str',
);

has port => (
  is  => 'ro',
  isa => 'Int',
);

has handle => (
  is        => 'rw',
  predicate => 'connected',
  clearer   => 'disconnect',
);

has timer => (
  is        => 'rw',
  clearer   => 'clear_timer',
);

has current_request => (
  is        => 'rw',
  clearer   => 'clear_current_request',
  predicate => 'waiting',
);

has [qw/ on_connect on_connect_error on_error on_eof /] => (
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

after disconnect => sub {
  my $self = shift;
  $self->clear_timer;
  $self->clear_current_request;
};


sub connect {
  my $self = shift;

  $self->disconnect;

  tcp_connect(
    $self->host,
    $self->port,
    sub {
      if (@_) {
        $self->_connect(@_);
      }
      else {
        my $cb = $self->on_connect_error;
        $cb->() if $cb;
      }
    },
    sub {
      $self->timeout / 1000;
    }
  );

  return $self;
}

sub _connect {
  my ($self, $fh) = @_;

  my $handle = AnyEvent::Handle->new(
    fh       => $fh,
    on_error => sub { warn "ERR"; $self->on_error },
    on_read  => sub {
      shift->unshift_read(
        chunk => 36,
        sub {
          return unless $self;

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

          my $req = $self->current_request;
          if ($req) {
            if ($req->request_id == $response_to) {
              $self->clear_current_request;
              $self->clear_timer;
              $self->flush_pending;
            }
            else {
              undef $req;
            }
          }
          my $cb = $req && $req->cb;

          if ($blen) {

            # We have documents to read
            shift->unshift_read(
              chunk => $blen,
              sub {
                if (!$req or $req->cancelled) {

                  # The cursor was cancelled after we sent a prefetch OP_GET_MORE
                  # So we need to just ignore this OP_RESULT and cancel if needed

                  $self->op_kill_cursors($request_id, $cursor_id)
                    if length($cursor_id);

                  return;
                }

                $req->received($doc_count);
                my $at          = $req->at;
                my $max         = $req->max;
                my $done        = $max && $at >= $max;
                my $do_prefetch = !$done && length($cursor_id) && $req->prefetch;

                my $page = do {
                  my $todo = $max && $max - $at;
                  my $p = $req->limit;
                  ($todo && $todo < $p) ? $todo : $p;
                };

                if ($do_prefetch) {

                  # User has requested prefetch, so lets send the OP_GET_MORE
                  # request before we start processing this one
                  $self->push_request($req->op_get_more($request_id, $cursor_id, $page));
                }

                my $want_more = $cb && $cb->(MongoDB::read_documents($_[1]));

                if ($want_more) {
                  if (length($cursor_id) and !$done) {
                    $self->push_request($req->op_get_more($request_id, $cursor_id, $page))
                      unless $do_prefetch;    # already sent
                  }
                  elsif ($page >= 0) {
                    $cb->() if $cb;         # signal finished
                  }
                }

                if (length($cursor_id) and ($done or !$want_more)) {
                  if ($do_prefetch) {

                    # We have already requested the next page of results,
                    # so will need to cancel when we get that
                    $req->cancel;
                  }
                  else {
                    $self->op_kill_cursors($request_id, $cursor_id);
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
      $self->disconnect;
      if (my $on_eof = $self->on_eof) {
        $on_eof->();
      }
    },
  );

  $self->handle($handle);
  if (my $on_connect = $self->on_connect) {
    $on_connect->($self);
  }
  $self->flush_pending;

  weaken($self); # avoid loops

  return;
}


sub flush_pending {
  my $self = shift;

  while (!$self->waiting and my $req = $self->shift_pending) {
    $self->send_request($req);
  }
}

sub send_request {
  my ($self, $req) = @_;

  return if $req->cancelled;
  if (my $cb = $req->cb) {
    $self->current_request($req);
    if (my $timeout = $req->timeout) {
      $self->timer(
        AE::timer(
          $timeout / 1000,
          0,
          sub {
            my $current = $self->current_request;
            if ($current and $current == $req) {
              $req->cancel;
              $cb->({'$err' => 'query timeout'});
            }
          }
        )
      );
    }
  }

  $self->handle->push_write(${ $req->message });

  weaken($self);

  return;
}

sub push_request {
  my $self = shift;
  my $req = shift;

  if ($self->has_pending or $self->waiting) {
    $self->push_pending($req);
  }
  else {
    $self->send_request($req);
  }
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

__PACKAGE__->meta->make_immutable;
