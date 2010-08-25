## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround
## ABSTRACT: Asynchronous MongoDB client using AnyEvent

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress, 
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB;

use Moose;
use Moose::Util::TypeConstraints;
use AnyEvent;
use Tie::IxHash;
use boolean;
use Scalar::Util qw(weaken);
use AnyEvent::MongoDB::Connection;
use AnyEvent::MongoDB::Database;
use AnyEvent::MongoDB::Collection;

use namespace::autoclean;

BEGIN {
  my $host_re = '[\-\.a-zA-Z0-9]+ (?: : \d+ )?';

  subtype 'MongoDBHost'    ##
    => as 'Str'            ##
    => where { $_ =~ m{^ $host_re $}x };

  subtype 'MongoDBHostStr'    ##
    => as 'Str'               ##
    => where { $_ =~ m{^mongodb:// $host_re (?: , $host_re)* $}x };
}

has host => (
  is      => 'ro',
  isa     => 'MongoDBHostStr',
  default => 'mongodb://localhost:27017',
);

# XXX need more callbacks, on_connect, on_connect_error, on_reconect??
has on_error => (
  is      => 'rw',
  isa     => 'CodeRef',
  default => sub {
    sub { confess(@_) }
  },
);

has auto_connect => (
  is      => 'ro',
  isa     => 'Bool',
  default => 1,
);

has auto_reconnect => (
  is      => 'ro',
  isa     => 'Bool',
  default => 1,
);

has timeout => (
  is      => 'ro',
  isa     => 'Int',
  default => 20000,
);

has w => (
    is      => 'rw',
    isa     => 'Int',
    default => 1,
);

has wtimeout => (
    is      => 'rw',
    isa     => 'Int',
    default => 1000,
);

has username => (
  is  => 'rw',
  isa => 'Str',
);

has password => (
  is  => 'rw',
  isa => 'Str',
);

has auth_db => (
  is      => 'rw',
  isa     => 'Str',
  default => 'admin',
);

has query_timeout => (
  is      => 'rw',
  isa     => 'Int',
  default => 30000,
);

has _connections => (
  traits  => ['Array'],
  isa     => 'ArrayRef[AnyEvent::MongoDB::Connection]',
  lazy_build => 1,
  handles => {                             ##
    _connections => 'elements',
  }
);

has master => (
  is         => 'ro',
  lazy_build => 1,
);


sub _build__connections {
  my $self = shift;
  (my $list = $self->host) =~ s,^mongodb://,,;

  my @conn = map {
    my ($host, $port) = split(/:/, $_);
    $port ||= 27017;
    AnyEvent::MongoDB::Connection->new(
      _mongo  => $self,
      host    => $host,
      port    => $port,
    );
  } split(',', $list);

  return \@conn;
}

sub connect {
  my $self = shift;
  # XXX this needs to be better. instead of just->handle, which cannot take params
  # we should have ->connect which can take cb and on_error
  foreach my $c ($self->_connections) {
    $c->handle;
  }
}

sub _build_master {
  my $self = shift;

  # XXX what about auto_{re,}connect
  # maybe conns need a meth ping { ->_has_handle or do connect }
  my @conn = grep { $_->connected } $self->_connections
    or confess "No connections";

  return $conn[0] if @conn == 1;

  # XXX only one condvar can be waited on at a time
  # this is an issue if we have 2 objects trying to determine
  # master at the same time. Also what if the cb is not called
  # by one and all others are slaves. we could wait forever
  # Need to find a better way to do this
  my $cv = AnyEvent->condvar;

  foreach my $c (@conn) {
    weaken(my $cc = $c);    # avoid loops
    $cv->begin;
    $c->is_master(
      sub {
        my $result = shift;
        ($result && $result->{ismaster}) ? $cv->send($cc) : $cv->end;
      }
    );
  }

  $cv->recv;
}

sub get_database {
  my ($self, $name) = @_;
  return AnyEvent::MongoDB::Database->new(
    _mongo => $self,
    name   => $name,
  );
}

__PACKAGE__->meta->make_immutable;

