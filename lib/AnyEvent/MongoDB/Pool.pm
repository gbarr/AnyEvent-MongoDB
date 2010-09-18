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

use namespace::autoclean;

has mongo => (
  is       => 'ro',
  isa      => MongoDB,
  required => 1,
  weak_ref => 1,
);

has _master => (
  is         => 'ro',
  isa        => Connection,
  lazy_build => 1,
);

has _connecting => (is => 'rw');

sub _build__master {
  my $self   = shift;
  my $mongo  = $self->mongo;
  my $master = Connection->new(mongo => $mongo);

  weaken(my $_self = $self);    # avoid loops
  # XXX need to handle not finding a master and connect errors
  # add support for server discovery from results
  my @connecting = map {
    Connection->new(
      host       => $_->{host},
      port       => $_->{port},
      mongo      => $self->mongo,
      on_connect => sub {
        my $conn = shift;
        weaken(my $_conn = $conn);    # avoid loops
        $_conn->op_query(
          { ns    => 'admin.$cmd',
            limit => -1,
            query => {ismaster => 1},
            cb    => sub {
              my $result = shift;
              if ($result->{ismaster}) {
                $master->set_fh($_conn->handle->fh);
                $_self->_connecting(undef);
              }
            },
          }
        );
      }
    )->connect;
  } $mongo->servers;

  $self->_connecting(\@connecting);

  return $master;
}

sub connect {
  my $self     = shift;
  my $slave_ok = shift;

  # XXX not really a pool right now.
  return $self->_master;
}

sub get_database {
  my ($self, $name) = @_;

  return Database->new(
    pool => $self,
    name => $name,
  );
}

__PACKAGE__->meta->make_immutable;
