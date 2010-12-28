## Copyright (C) Graham Barr
## vim: ts=8:sw=2:expandtab:shiftround

## WARNING!!!!
## WARNING!!!! This code is still very much a work in progress,
## WARNING!!!! do not depend on anything not changing
## WARNING!!!!

package AnyEvent::MongoDB::GridFS::File;

use Moose;

has doc => ( is => 'ro');
has gridfs => ( is => 'ro' );

use Scalar::Util qw(weaken);

use namespace::autoclean;

sub read {
  my $self = shift;

  my %arg;
  $arg{cb}     = shift if ref($_[0]);

  my %options = (@_, %arg);

  # TODO, support fetching less than just the whole file

  my $files_id = $self->doc->{_id};
  my $cb = $options{cb};
  my $chunks = $self->gridfs->chunks;
  my $buffer;
  my $n = 0;

  my $iter;
  $iter = sub {
    my $doc = shift;
    if (!$doc) {
      $cb->($options{chunked} ? () : ($buffer));    # done
    }
    elsif ($doc->{'$err'}) {
      $cb->($doc);       # done
    }
    else {
      my $more = 1;
      if ($options{chunked}) {
        $more = $cb->($doc->{data});
      }
      else {
        $buffer .= $doc->{data};
      }
      if ($more) {
        $chunks->find(
          query => {
            files_id => $files_id,
            n        => ++$n,
          },
          cb => $iter,
          limit => -1,
        );
      }
    }
    return;
  };

  $chunks->find(
    query => {
      files_id => $files_id,
      n        => $n,
    },
    cb => $iter,
  );

  weaken($iter); # avoid loops
}

__PACKAGE__->meta->make_immutable;
