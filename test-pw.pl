#!/usr/bin/perl
#
# Reads a list of usernames from STDIN and checks which have not changed their default password
#

use LWP::UserAgent;
use JSON::PP;

my $ua = LWP::UserAgent->new;
my $server_endpoint = "https://backend.deqar.eu/accounts/get_token/";
my $req = HTTP::Request->new(POST => $server_endpoint);
$req->header('content-type' => 'application/json');

my @changed;
my @unchanged;

print("Trying");

while (<STDIN>) {
	chomp;
	my $post_data = encode_json({"username" => $_, "password" => $_ . '#2018'});
	# add POST data to HTTP request body
	$req->content($post_data);
	# now we make the request
	my $resp = $ua->request($req);
	if ($resp->is_success) {
		push(@unchanged, $_);
	} else {
		push(@changed, $_);
	}
	print ".";
}
print "\n";

print "Default passwords: ".join(", ",@unchanged)."\n";

print "Passwords changed or users unknown: ".join(", ",@changed)."\n";
