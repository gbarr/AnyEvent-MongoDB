## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Database;

use Moose;
use Digest::MD5 qw(md5_hex);
use Scalar::Util qw(weaken);

use aliased 'AnyEvent::MongoDB::Pool';
use aliased 'AnyEvent::MongoDB::Collection';
use aliased 'AnyEvent::MongoDB::Connection';
use aliased 'AnyEvent::MongoDB::Request';

use namespace::autoclean;

has pool => (
  is       => 'ro',
  isa      => Pool,
  required => 1,
  handles  => [qw/ mongo /],
);

has name => (
  is       => 'ro',
  isa      => 'Str',
  required => 1,
);

has connection => (
  is        => 'rw',
  isa       => Connection,
  clearer   => 'clear_connection',
);

has pending => (
  traits  => ['Array'],
  default => sub { [] },
  handles => {
    shift_pending => 'shift',
    push_pending  => 'push',
    has_pending   => 'count',
  },
);

sub _exec {
  my ($self, $op, $options) = @_;

  $options->{safe} ||= 1
    if $op ne 'op_query' and $options->{cb};

  if ($options->{safe}) {
    my $m = $self->mongo;
    $options->{w}        ||= $m->w;
    $options->{wtimeout} ||= $m->wtimeout;
  }

  $options->{timeout} = $self->mongo->query_timeout
    if !exists $options->{timeout} and ($op eq 'op_query' or $options->{safe});

  my $req = Request->$op($options);

  # XXX TODO slave_ok

  my $conn = $self->connection;
  if ($conn and $conn->connected) {
    $conn->push_request($req);
  }
  else {
    $self->push_pending($req);
    if ($self->has_pending == 1) {
      $self->pool->connect(
        sub {
          my $conn = shift;
          $self->connection($conn);
          while (my $r = $self->shift_pending) {
            $conn->push_request($r);
          }
        }
      );
    }
  }

  return $req;
}

sub get_collection {
  my ($self, $name) = @_;

  return Collection->new(
    database => $self,
    name     => $name,
  );
}

sub authenticate {
  my ($self, $user, $pass, %options) = @_;

  my $save     = exists $options{save} ? $options{save} : 1;
  my $cb       = $options{cb};
  my $on_error = $options{on_error} || $self->mongo->on_auth_error || $self->mongo->on_error;

  # create a hash if the password isn't yet encrypted
  $pass = md5_hex("${user}:mongo:${pass}")
    unless $options{is_digest};

  $self->run_command(
    {getnonce => 1},
    sub {
      my $doc = shift;
      unless ($doc->{ok} and $doc->{nonce}) {
        $on_error->($doc) if $on_error;
        return;
      }
      my $nonce = $doc->{nonce};
      $self->run_command(
        Tie::IxHash->new(
          authenticate => 1,
          user         => $user,
          nonce        => $nonce,
          key          => md5_hex($nonce . $user . $pass),
        ),
        sub {
          my $doc = shift;
          $self->mongo->save_auth($self->name, $user => $pass) if $save and $doc->{ok};
          my $cb = $options{cb};
          $cb->($doc) if $cb;
        }
      );
    }
  );

  weaken(my $_self = $self);    # avoid loops
}

sub last_error {
  my $self = shift;

  my %arg;
  $arg{cb} = shift if ref($_[0]);

  my %options = (@_, %arg);
  if (my $conn = $self->connection) {
    $conn->last_error(\%options);
  }
  elsif (my $cb = $arg{cb}) {
    $cb->({err => "no connection"});
  }
}


sub run_command {
  my $self = shift;

  my %arg = (limit => -1, skip => 0, ns => $self->name . '.$cmd');
  $arg{command} = shift if ref($_[0]);
  $arg{cb}      = shift if ref($_[0]);

  my %options = (@_, %arg);
  $self->_exec(op_query => \%options);
}


__PACKAGE__->meta->make_immutable;
