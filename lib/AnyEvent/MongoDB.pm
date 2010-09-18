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

use aliased 'AnyEvent::MongoDB::Pool';

use AnyEvent;
use Tie::IxHash;
use boolean;

use namespace::autoclean;

BEGIN {
  my $hostport = '[\-\.a-zA-Z0-9]+ (?: : \d+ )?';
  my $userpass = '[-_.\w\d]+:[-_\w\d]+@';
  my $dbname   = '[-\d\w]+';

  subtype 'MongoDB_URI'    ##
    => as 'Str'            ##
    => where {
    $_ =~ m{
      ^mongodb://
      (?:
        (?: $userpass ) $hostport (?: , $hostport)* (?: /$dbname )
        |  $hostport (?: , $hostport)*
      )
      $}x;
    };
}

has uri => (
  is  => 'ro',
  isa => 'MongoDB_URI',
);

has host => (
  is      => 'ro',
  isa     => 'Str',
  default => '127.0.0.1',
);

has port => (
  is      => 'ro',
  isa     => 'Int',
  default => 27017,
);

has servers => (
  is         => 'ro',
  isa        => 'ArrayRef',
  lazy_build => 1,
  auto_deref => 1,
);

# XXX need more callbacks, on_connect, on_connect_error, on_reconect??
has on_error => (
  is      => 'rw',
  isa     => 'CodeRef',
  default => sub {
    sub { confess(@_) }
  },
);

has on_auth_error => (
  is  => 'rw',
  isa => 'CodeRef',
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

has pool => (
  is         => 'rw',
  isa        => Pool,
  lazy_build => 1,
  handles    => ['get_database'],
);

sub _build_servers {
  my $self = shift;
  my @nodes;
  if (my $uri = $self->uri) {
    my ($user, $pass, $db);
    $uri =~ s,^mongodb://,,;
    ($user, $pass) = ($1, $2) if $uri =~ s,([-_.\w\d]+):([-_\w\d]+)@,,;
    ($db) = $1 if $uri =~ s,/([^/]+)$,,;
    $self->add_auth($user, $pass, $db) if defined $user;
    foreach my $part (split(/,/, $uri)) {
      my ($host, $port) = split(/:/, $part);
      push @nodes, {host => $host, port => $port || 27017};
    }
  }
  else {
    @nodes = ({host => $self->host, port => $self->port});
  }

  return \@nodes;
}

sub BUILD {
  my $self = shift;

  $self->clear_servers;
  $self->servers;

  my ($user, $pass, $db) = ($self->username, $self->password, $self->auth_db);
  $self->add_auth($user, $pass, $db) if defined $user;

  return;
}

sub _build_pool {
  my $self = shift;

  return Pool->new(mongo => $self);
}

sub add_auth {

  #XXX TODO
}

__PACKAGE__->meta->make_immutable;

