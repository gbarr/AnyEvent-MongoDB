## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Pool;

use Moose;
use boolean;

use Scalar::Util qw(weaken);

use aliased 'AnyEvent::MongoDB';
use aliased 'AnyEvent::MongoDB::Connection';
use aliased 'AnyEvent::MongoDB::Database';
use aliased 'AnyEvent::MongoDB::Request';

use namespace::autoclean;

has mongo => (
  is       => 'ro',
  isa      => MongoDB,
  required => 1,
  weak_ref => 1,
);

has master => (
  is       => 'ro',
  init_arg => undef,
  writer   => '_set_master',
  isa      => Connection,
);

has _connecting => (is => 'rw');

sub connect {
  my $self     = shift;
  my $cb = pop;

  # XXX not really a pool right now.
  my $master = $self->master;
  if ($master and $master->connected) {
    $cb->($master);
    return;
  }

  my $mongo  = $self->mongo;

  # XXX need to handle not finding a master and connect errors
  # add support for server discovery from results
  my @connecting = map {
    Connection->new(
      host       => $_->{host},
      port       => $_->{port},
      mongo      => $self->mongo,
      on_connect => sub {
        my $conn = shift;

        $conn->push_request(
          Request->op_query(
            { ns    => 'admin.$cmd',
              limit => -1,
              query => {ismaster => 1},
              cb    => sub {
                my $result = shift;
                if ($result->{ismaster}) {
                  $self->_set_master($conn);
                  $cb->($conn);
                }
              },
            }
          )
        );
        weaken($conn);    # avoid loops
      }
    )->connect;
  } $mongo->servers;

  $self->_connecting(\@connecting);

  weaken($self);    # avoid loops

  return;
}

sub get_database {
  my ($self, $name) = @_;

  return Database->new(
    pool => $self,
    name => $name,
  );
}

__PACKAGE__->meta->make_immutable;
