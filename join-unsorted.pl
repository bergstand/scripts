#!/usr/bin/perl -w

#reads one file on STDIN, another one as argument, adds columns from the second file into the first one by matching the field specified by the user
#all indicies 1-based
#example syntax:
#cat <input file> | perl ~/programs/join-unsorted.pl --first 1 --second 1 --keep 2,3 --insert 2 --includeHeader <file to add from>
#can also match on multiple columns, e.g.: --first 1,2,3,4

use strict;
use Getopt::Long;
use Data::Dumper;

my $firstCol;	#matching column for the first file
my $secondCol;	#matching column for the second file
my $keepCols;	#which columns in the second file to keep - comma-separated string
my $insertCol;	#BEFORE which column in the first file to insert the new data (1 = before first column)
my $includeHeader;	#tries to carry over header from the second file into the first file
my $prefixFirstHeader;	#adds a string and a period in front of each header column in the first file: "file1."
my $prefixSecondHeader;	#adds a string and a period in front of each header column in the second file: "file2."
my $fill01;	#instead of filling with actual values from the second file, simply add "1" if there is a match and "0" if not

GetOptions ("first=s" => \$firstCol, "second=s" => \$secondCol, "keep=s" => \$keepCols, "insert=i" => \$insertCol, "includeheader" => \$includeHeader, "prefixFirstHeader=s" => \$prefixFirstHeader, "prefixSecondHeader=s" => \$prefixSecondHeader, "fill01" => \$fill01);

my @firstCol = split(/,/,$firstCol); map {$_--} @firstCol;
my @secondCol = split(/,/,$secondCol); map {$_--} @secondCol;	#-1 because Perl is 0-based and script interface is 1-based

if ($fill01) {
	#if fill01 is given, the --keepCols argument is not needed. We are not keeping any columns, simply indicating the presence or absence of a given row in the second file. So arbitrarily set keepCols to (1)
	$keepCols = 1;
}

my $data = $ARGV[0];	#file with data to add
my ($dataRef,$header) = readData($data,\@secondCol,$keepCols);
my %data = %{$dataRef};

#if specified, prefix all header columns from the second file
if ($prefixSecondHeader) {
	$header = prefixHeader($header,$prefixSecondHeader);
}

#determine the number of columns to keep - will be needed to fill in the corrent number of NA's in case of missing data
my $nrKeepCols=split(/,/,$keepCols);

#read input file
my $c=0;
while(my $line = <STDIN>) {
	chomp($line);
	my @line = split(/\t/,$line);

	if ($includeHeader && $c == 0) {
		#if specified, prefix all header columns from the first file
		if ($prefixFirstHeader) {
			my $headerLine = prefixHeader(join("\t",@line),$prefixFirstHeader);
			@line = split("\t",$headerLine);
		}
		splice @line, $insertCol-1, 0, $header;
        print join("\t",@line),"\n";
		
	} else {
		#look for saved data
		my $matchCol = join("\t",@line[@firstCol]);
		
		#initialise with the appropriate number of NAs
		my $newData = "NA"."\tNA"x($nrKeepCols-1);
		if (defined($fill01)) {
			$newData = "0"."\t0"x($nrKeepCols-1);	#use 0 instead of NA if fill01
		}
		
		if (defined($data{$matchCol})) {
			if (defined($fill01)) {
				$newData = "1"."\t1"x($nrKeepCols-1);	#use the appropriate number of columns
			} else {
				$newData = $data{$matchCol};
			}
		}
	
		#insert new data
		splice @line, $insertCol-1, 0, $newData;
		print join("\t",@line),"\n";
	}

	$c++;
}

exit;

sub readData {
	my ($file,$matchCol,$keepCols) = @_;
	open(DATA, $file) || die "Could not read data file $file.\n";
	
	my @keepCols = split(/,/,$keepCols);
	@keepCols = map($_ - 1,@keepCols);	#perl is 0-based, script interface is 1-based
	my @matchCol = @{$matchCol};	#these have already been mapped to 0-based upstream

	my %data = ();	#hash of strings
	my $header = "NA";
	my $c = 0;
	while (my $line = <DATA>) {
		chomp($line);
		my @line = split(/\t/,$line);
		my $value = join("\t",@line[@matchCol]);
		my @keepValues = @line[@keepCols];

		if ($includeHeader) {
			if ($c == 0) {
				$header = join("\t",@keepValues);
			} else {
				$data{$value} = join("\t",@keepValues);
			}
		} else {
			$data{$value} = join("\t",@keepValues);
		}

		$c++;
	}
	
	return (\%data,$header);

}

sub prefixHeader {
	my ($header,$prefixSecondHeader) = @_;

	my @header = split(/\t/,$header);
	map($_ = "$prefixSecondHeader.".$_,@header);
	$header = join("\t",@header);
	
	return $header;
}
