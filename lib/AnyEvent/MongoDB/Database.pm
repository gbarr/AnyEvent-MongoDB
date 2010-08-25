## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress, 
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Database;

use Moose;

has _mongo => (
  is => 'ro',
  required => 1,
);

has name => (
    is       => 'ro',
    isa      => 'Str',
    required => 1,
);

sub get_collection {
  my ($self, $name) = @_;
  return AnyEvent::MongoDB::Collection->new(
    _database => $self,
    name      => $name,
  );
}

sub last_error {
  shift->_mongo->master->last_error(@_);
}


sub run_command {
  my ($self, $command, $cb) = @_;
  $self->get_collection('$cmd')->find_one(query => $command, cb => $cb);
}

__PACKAGE__->meta->make_immutable;
