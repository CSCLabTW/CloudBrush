#!/usr/bin/perl -w
use strict;
use Getopt::Long;

my $HELP   = 0;
my $suffix = "";
my $renum  = undef;
my $prefix = undef;

my $result = GetOptions(
  "h"        => \$HELP,
  "suffix=s" => \$suffix,
  "renum"    => \$renum,
);

if ($HELP)
{
  print "preprocess.pl [options] fastq > sfa\n";
  print " -h            : Help\n";
  print " -renum        : renumber 1,2,..,E,F,10,11,12..\n";
  print " -suffix <str> : add suffix to each readname\n";
  exit  0;
}

my $readcnt = 0;

sub printSeq
{
  my $tag = shift;
  my $seq = shift;

  return if !defined $tag;
  return if !defined $seq;

  chomp($seq);
  $seq = uc($seq);

  $readcnt++;

  if ($renum)
  {
    printf "%X$suffix\t$seq\n", $readcnt;
  }
  else
  {
    $tag =~ s/\s+/_/g;
    $tag =~ s/[:#-\.\/]/_/g;
    $tag .= $suffix;

    print "$tag\t$seq\n";
  }
}

my $tag = undef;
my $seq = undef;

my $inseq = 0;

while(<>)
{
  next if /^#/;
  next if /^\s*$/;
  next if /^\+/;

  if (/^@(\S+)/)
  {
    printSeq($tag, $seq);

    ## Parse fastq
    $tag = $1;
    $seq = <>;

    my $h2 = <>;
    my $qual = <>;
  }
  elsif (/^>(\S+)/)
  {
    printSeq($tag, $seq);

    ## Parse fasta
    $tag = $1;
    $seq = "";
    $inseq = 1;
  }
  elsif ($inseq)
  {
    chomp;
    $seq .= $_;
  }
  else
  {
    print STDERR "WARNING line $.: Invalid file format in $tag expected > or @ saw \"$_\"";

    $tag = undef;
    $seq = undef;
  }
}

printSeq($tag,$seq);

print STDERR "processed $readcnt reads\n";
