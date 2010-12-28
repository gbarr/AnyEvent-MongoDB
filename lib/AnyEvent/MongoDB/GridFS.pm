## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::GridFS;

use Moose;
use DateTime;

use aliased 'AnyEvent::MongoDB::GridFS::File' => 'GridFSFile';
use aliased 'AnyEvent::MongoDB::Database';

has prefix => (
  is      => 'ro',
  isa     => 'Str',
  default => 'fs',
);

has database => (
  is       => 'ro',
  isa      => Database,
  required => 1,
);

has files => (
  is         => 'ro',
  init_arg   => undef,
  lazy_build => 1,
);

sub _build_files {
  my $self = shift;
  $self->database->get_collection($self->prefix . ".files");
}

has chunks => (
  is         => 'ro',
  init_arg   => undef,
  lazy_build => 1,
);

sub _build_chunks {
  my $self = shift;
  $self->database->get_collection($self->prefix . ".chunks");
}


sub fetch {
  my $self = shift;

  my %arg;
  $arg{query}  = shift if ref($_[0]);
  $arg{cb}     = shift if ref($_[0]);

  my %options = (@_, %arg);
  my $cb = $options{cb};
  $options{cb} = sub {
    $cb->(map { GridFSFile->new(doc => $_, gridfs => $self) } @_);
  };

  $self->files->find(%options);
}

__PACKAGE__->meta->make_immutable;
