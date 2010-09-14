## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Collection;

use Moose;
use boolean;

use aliased 'AnyEvent::MongoDB::Database';

use namespace::autoclean;

has database => (
  is       => 'ro',
  isa      => Database,
  required => 1,
  handles  => [qw/ _exec /],
);

has ns => (
  is         => 'ro',
  lazy_build => 1,
);

has name => (
  is       => 'ro',
  isa      => 'Str',
  required => 1,
);

sub _build_ns {
  my $self = shift;
  join ".", $self->database->name, $self->name;
}

sub find {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{query}  = shift if ref($_[0]);
  $arg{fields} = shift if ref($_[0]);
  $arg{cb}     = shift if ref($_[0]);

  my %options = (@_, %arg);
  # XXX if slave_okay then we could pick a slave
  $self->_exec(op_query => \%options);
}

sub find_one {
  shift->find(@_, limit => -1, skip => 0);
}

sub insert {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{documents} = shift if ref($_[0]);
  $arg{cb}        = shift if ref($_[0]);

  my %options = (@_, %arg);

  $options{documents} ||= delete $options{document};

  $options{documents} = [$options{documents}]
    unless ref($options{documents}) eq 'ARRAY';

  $self->_exec(op_insert => \%options);
}

sub save {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{document} = shift if ref($_[0]);
  $arg{cb}       = shift if ref($_[0]);

  my %options = (@_, %arg);

  my $id = $options{document}{_id};
  if (defined $id) {
    $options{query} = {_id => $id};
    $options{upsert} = true;
    $self->_exec(op_update => \%options);
  }
  else {
    $self->_exec(op_insert => \%options);
  }
}

sub update {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{query} = shift if ref($_[0]);
  $arg{doc}   = shift if ref($_[0]);
  $arg{cb}    = shift if ref($_[0]);

  my %options = (@_, %arg);

  $self->_exec(op_update => \%options);
}

sub remove {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{query} = shift if ref($_[0]);
  $arg{cb}    = shift if ref($_[0]);

  my %options = (@_, %arg);

  $self->_exec(op_remove => \%options);
}

sub ensure_index {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{key} = shift if ref($_[0]);
  $arg{cb}  = shift if ref($_[0]);

  my %options = (@_, %arg);

  $options{name} ||= do {
    my @k = $arg{key}->Keys;
    my @v = $arg{key}->Values;
    join("_", map { $k[$_], $v[$_] } 0 .. $#k);
  };

  my $doc = Tie::IxHash->new(
    name       => delete $options{name},
    ns         => delete $options{ns},
    key        => delete $options{key},
    unique     => delete $options{unique} ? true : false,
    dropDups   => delete $options{drop_dups} ? true : false,
    background => delete $options{background} ? true : false,
  );

  $options{documents} = [ $doc ];
  $options{ns} = "system.indexes";

  $self->_exec(op_insert => \%options);
}

sub drop_index {
  my $self  = shift;
  my $index = shift;

  my $cmd = Tie::IxHash->new(
    deleteIndexes => $self->name,
    index         => $index,
  );

  $self->database->run_command($cmd, @_);
}

sub drop_indexes {
  shift->drop_index('*', @_);
}

sub drop {
  my $self = shift;

  $self->database->run_command({drop => $self->name}, @_);
}

sub validate {
  my $self = shift;

  $self->database->run_command({validate => $self->name}, @_);
}

sub get_indexes {
  my $self = shift;

  my %arg = (ns => "system.indexes");
  $arg{cb}  = shift if ref($_[0]);

  my %options = (@_, %arg);
  $options{query} = { ns => $self->ns };

  $self->_exec(op_query => \%options);
}

sub count {
  my $self = shift;

  my %arg = (ns => $self->ns);
  $arg{query} = shift if ref($_[0]);
  $arg{cb}    = shift if ref($_[0]);

  my %options = (@_, %arg);

  my $cmd = Tie::IxHash->new(
    count => $self->name,
    query => delete $options{query} || {},
  );

  $self->database->run_command($cmd, %options);
}

__PACKAGE__->meta->make_immutable;

