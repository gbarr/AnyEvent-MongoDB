## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::Request;

use Moose;

has message => (
  isa      => 'ScalarRef',
  is       => 'ro',
  required => 1,
);

has cb         => (is => 'ro');
has ns         => (is => 'ro', required => 1, isa => 'Str');
has max        => (is => 'ro');
has prefetch   => (is => 'ro');
has timeout    => (is => 'ro');
has limit      => (is => 'ro');

has ids => (    # Only used for op_insert
  isa        => 'ArrayRef',
  traits     => ['Array'],
  lazy_build => 1,
  handles    => {             ##
    ids => 'elements',
  },
);
sub _build_ids { [] }

has request_id => (
  is     => 'ro',
  writer => '_set_request_id',
);

has at => (
  traits  => ['Number'],
  is      => 'ro',
  isa     => 'Num',
  default => 0,
  init_arg => undef,
  handles => {             ##
    received => 'add',
  },
);
  
has cancelled => (
  traits   => ['Bool'],
  is       => 'ro',
  isa      => 'Bool',
  init_arg => undef,
  default  => 0,
  handles  => {           ##
    cancel => 'set',
  },
);


sub op_query {
  my ($self, $opt) = @_;

  my $flags = ($opt->{tailable} ? 1 << 1 : 0)    ##
    | ($opt->{slave_okay} ? 1 << 2 : 0)          ##
    | ($opt->{immortal}   ? 1 << 4 : 0);

  my $limit = $opt->{limit} || 0;
  my $page  = $opt->{page}  || 100;

  $page = $limit if $limit and $limit < $page;

  my ($query, $info) = MongoDB::write_query(
    $opt->{ns},    ##
    $flags,        ##
    $opt->{skip} || 0,
    $page,
    $opt->{query}  || {},
    $opt->{fields} || {}
  );

  # TODO: $opt->{all} - fetch all documents before calling cb, just once
  $info->{exhaust}  = $opt->{exhaust};                                  # TODO
  $info->{prefetch} = defined $opt->{prefetch} ? $opt->{prefetch} : 1;
  $info->{cb}       = $opt->{cb};
  $info->{max}      = abs($limit);
  $info->{timeout}  = $opt->{timeout};

  # MongoDB returns an alias to the scalar in $info hash,
  # which means it changes the next time $MongoDB::Cursor::_request_id
  # is incremented. but we want a copy
  $info->{request_id} = 0 + delete $info->{request_id};
  $info->{message} = \$query;

  $self->new($info);
}


sub op_update {
  my ($self, $opt) = @_;

  my $flags = ($opt->{upsert} ? 1 : 0) | ($opt->{multiple} ? 1 << 1 : 0);

  my ($update) = MongoDB::write_update(    ##
    $opt->{ns},
    $opt->{query}  || {},
    $opt->{update} || {},
    $flags,
  );

  $self->new({ %$opt, message => \$update });
}

sub op_insert {
  my ($self, $opt) = @_;

  my ($insert, $ids) = MongoDB::write_insert($opt->{ns}, $opt->{documents});

  $self->new({ %$opt, message => \$insert, ids => $ids });
}

sub op_delete {
  my ($self, $opt) = @_;

  my ($delete) = MongoDB::write_remove($opt->{ns}, $opt->{query}, $opt->{just_one} || 0);

  $self->new({%$opt, message => \$delete });
}

sub op_get_more {
  my ($self, $request_id, $cursor_id, $page) = @_;

  my $next_request_id = ++$MongoDB::Cursor::_request_id;
  my $message         = pack(
    "V V V V   V Z* V a8",
    0, $next_request_id, $request_id, 2005,        # OP_GET_MORE
    0, $self->ns,        $page,       $cursor_id
  );
  substr($message, 0, 4) = pack("V", length($message));
  ${$self->message} = $message;
  $self->_set_request_id($next_request_id);

  return $self;
}

sub BUILDARGS {
  my $class = shift;
  my $args  = $class->SUPER::BUILDARGS(@_);

  if ($args->{safe}) {
    my $last_error = Tie::IxHash->new(
      getlasterror => 1,
      w            => int($args->{w} || 1),
      wtimeout     => int($args->{wtimeout} || 1000),
    );
    (my $db = $args->{ns}) =~ s/\..*//;
    my ($query, $info) = MongoDB::write_query($db . '.$cmd', 0, 0, -1, $last_error);
    @{$args}{keys %$info} = values %$info;

    ${$args->{message}} .= $query;
  }

  return $args;
}

__PACKAGE__->meta->make_immutable;
