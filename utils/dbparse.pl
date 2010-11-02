#!/usr/bin/perl
while (<STDIN>) {
	if (/^#define ([^\s]+)\s.*DBTEXT:\s+(.*)/) {
		print "{ $1, \"$2\"},\n";
	}
}
