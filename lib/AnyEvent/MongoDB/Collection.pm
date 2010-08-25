## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress, 
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Collection;

use Moose;

has _database => (
  is       => 'ro',
  required => 1,
);

has _ns => (
  is         => 'ro',
  lazy_build => 1,
);

has name => (
  is       => 'ro',
  isa      => 'Str',
  required => 1,
);

sub _build__ns {
  my $self = shift;
  join ".", $self->_database->name, $self->name;
}

sub find {
  my $self = shift;
  # XXX if slave_okay then we could pick a slave
  $self->_database->_mongo->master->op_query(@_, ns => $self->_ns);
}

sub find_one {
  my $self = shift;
  # XXX if slave_okay then we could pick a slave
  $self->_database->_mongo->master->op_query(@_, ns => $self->_ns, limit => -1, skip => 0);
}

sub insert {
  my $self = shift;
  $self->_database->_mongo->master->op_insert(@_, ns => $self->_ns);
}

sub update {
  my $self = shift;
  $self->_database->_mongo->master->op_update(@_, ns => $self->_ns);
}

sub remove {
  my $self = shift;
  $self->_database->_mongo->master->op_remove(@_, ns => $self->_ns);
}


__PACKAGE__->meta->make_immutable;

