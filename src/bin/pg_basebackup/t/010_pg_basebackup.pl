
# Copyright (c) 2021, PostgreSQL Global Development Group

use strict;
use warnings;
use Cwd;
use Config;
use File::Basename qw(basename dirname);
use File::Path qw(rmtree);
use PostgresNode;
use TestLib;
use Test::More tests => 109 + 21;

program_help_ok('pg_basebackup');
program_version_ok('pg_basebackup');
program_options_handling_ok('pg_basebackup');

my $tempdir = TestLib::tempdir;

my $node = get_new_node('main');

# Set umask so test directories and files are created with default permissions
umask(0077);

# Initialize node without replication settings
$node->init(extra => ['--data-checksums']);
$node->start;
my $pgdata = $node->data_dir;

$node->command_fails(['pg_basebackup', '--target-gp-dbid', '123'],
	'pg_basebackup needs target directory specified');

# Some Windows ANSI code pages may reject this filename, in which case we
# quietly proceed without this bit of test coverage.
if (open my $badchars, '>>', "$tempdir/pgdata/FOO\xe0\xe0\xe0BAR")
{
	print $badchars "test backup of file with non-UTF8 name\n";
	close $badchars;
}

$node->set_replication_conf();
$node->reload;

command_fails(['pg_basebackup', '-D', "$tempdir/backup" ],
	'pg_basebackup fails without specifiying the target cloudberry db id');

$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123' ],
	'pg_basebackup fails because of WAL configuration');

ok(!-d "$tempdir/backup", 'backup directory was cleaned up');

# Create a backup directory that is not empty so the next command will fail
# but leave the data directory behind
mkdir("$tempdir/backup")
  or BAIL_OUT("unable to create $tempdir/backup");
append_to_file("$tempdir/backup/dir-not-empty.txt", "Some data");

$node->command_fails([ 'pg_basebackup', '-D', "$tempdir/backup", '-n' ],
	'failing run with no-clean option');

ok(-d "$tempdir/backup", 'backup directory was created and left behind');
rmtree("$tempdir/backup");

open my $conf, '>>', "$pgdata/postgresql.conf";
print $conf "max_replication_slots = 10\n";
print $conf "max_wal_senders = 10\n";
print $conf "wal_level = replica\n";
close $conf;
$node->restart;

# Write some files to test that they are not copied.
foreach my $filename (
	qw(backup_label tablespace_map postgresql.auto.conf.tmp
	current_logfiles.tmp global/pg_internal.init.123))
{
	open my $file, '>>', "$pgdata/$filename";
	print $file "DONOTCOPY";
	close $file;
}

# Connect to a database to create global/pg_internal.init.  If this is removed
# the test to ensure global/pg_internal.init is not copied will return a false
# positive.
$node->safe_psql('postgres', 'SELECT 1;');

# Create an unlogged table to test that forks other than init are not copied.
$node->safe_psql('postgres', 'CREATE UNLOGGED TABLE base_unlogged (id int)');

my $baseUnloggedPath = $node->safe_psql('postgres',
	q{select pg_relation_filepath('base_unlogged')});

# Make sure main and init forks exist
ok(-f "$pgdata/${baseUnloggedPath}_init", 'unlogged init fork in base');
ok(-f "$pgdata/$baseUnloggedPath",        'unlogged main fork in base');

# Create files that look like temporary relations to ensure they are ignored.
my $postgresOid = $node->safe_psql('postgres',
	q{select oid from pg_database where datname = 'postgres'});

# The name pattern of temp relation file is different from upstream, see function looks_like_temp_rel_name
my @tempRelationFiles =
  qw(t_999 t_999.1 t_9999_vm t_99999_vm.1);

foreach my $filename (@tempRelationFiles)
{
	append_to_file("$pgdata/base/$postgresOid/$filename", 'TEMP_RELATION');
}

# Run base backup.
$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/backup", '-X', 'none', '--target-gp-dbid', '123' ],
	'pg_basebackup runs');
ok(-f "$tempdir/backup/PG_VERSION",      'backup was created');
ok(-f "$tempdir/backup/backup_manifest", 'backup manifest included');

# Permissions on backup should be default
SKIP:
{
	skip "unix-style permissions not supported on Windows", 1
	  if ($windows_os);

	ok(check_mode_recursive("$tempdir/backup", 0700, 0600),
		"check backup dir permissions");
}

# Only archive_status directory should be copied in pg_wal/.
is_deeply(
	[ sort(slurp_dir("$tempdir/backup/pg_wal/")) ],
	[ sort qw(. .. archive_status) ],
	'no WAL files copied');

# Contents of these directories should not be copied.
foreach my $dirname (
	qw(pg_dynshmem pg_notify pg_replslot pg_serial pg_snapshots pg_stat_tmp pg_subtrans)
  )
{
	is_deeply(
		[ sort(slurp_dir("$tempdir/backup/$dirname/")) ],
		[ sort qw(. ..) ],
		"contents of $dirname/ not copied");
}

# These files should not be copied.
foreach my $filename (
	qw(postgresql.auto.conf.tmp postmaster.opts postmaster.pid tablespace_map current_logfiles.tmp
	global/pg_internal.init global/pg_internal.init.123))
{
	ok(!-f "$tempdir/backup/$filename", "$filename not copied");
}

# Unlogged relation forks other than init should not be copied
ok(-f "$tempdir/backup/${baseUnloggedPath}_init",
	'unlogged init fork in backup');
ok( !-f "$tempdir/backup/$baseUnloggedPath",
	'unlogged main fork not in backup');

# Temp relations should not be copied.
foreach my $filename (@tempRelationFiles)
{
	ok( !-f "$tempdir/backup/base/$postgresOid/$filename",
		"base/$postgresOid/$filename not copied");
}

# Make sure existing backup_label was ignored.
isnt(slurp_file("$tempdir/backup/backup_label"),
	'DONOTCOPY', 'existing backup_label not copied');
rmtree("$tempdir/backup");

$node->command_ok(
	[
		'pg_basebackup',    '-D',
		"$tempdir/backup2", '--no-manifest',
		'--waldir',         "$tempdir/xlog2",
		'--target-gp-dbid', '123'
	],
	'separate xlog directory');
ok(-f "$tempdir/backup2/PG_VERSION",       'backup was created');
ok(!-f "$tempdir/backup2/backup_manifest", 'manifest was suppressed');
ok(-d "$tempdir/xlog2/",                   'xlog directory was created');
rmtree("$tempdir/backup2");
rmtree("$tempdir/xlog2");

$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/tarbackup", '--target-gp-dbid', '123', , '-Ft' ],
	'tar format');
ok(-f "$tempdir/tarbackup/base.tar", 'backup tar was created');
rmtree("$tempdir/tarbackup");

$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-T=/foo" ],
	'-T with empty old directory fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-T/foo=" ],
	'-T with empty new directory fails');
$node->command_fails(
	[
		'pg_basebackup', '-D', "$tempdir/backup_foo", '-Fp',
		"-T/foo=/bar=/baz", '--target-gp-dbid', '123'
	],
	'-T with multiple = fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-Tfoo=/bar" ],
	'-T with old directory not absolute fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-T/foo=bar" ],
	'-T with new directory not absolute fails');
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup_foo", '--target-gp-dbid', '123', '-Fp', "-Tfoo" ],
	'-T with invalid format fails');

# Tar format doesn't support filenames longer than 100 bytes.
my $superlongname = "superlongname_" . ("x" x 100);
my $superlongpath = "$pgdata/$superlongname";

open my $file, '>', "$superlongpath"
  or die "unable to create file $superlongpath";
close $file;
$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/tarbackup_l1", '--target-gp-dbid', '123', '-Ft' ],
	'pg_basebackup tar with long name fails');
unlink "$pgdata/$superlongname";

# The following tests are for symlinks.

# Move pg_replslot out of $pgdata and create a symlink to it.
$node->stop;

# Set umask so test directories and files are created with group permissions
umask(0027);

# Enable group permissions on PGDATA
chmod_recursive("$pgdata", 0750, 0640);

rename("$pgdata/pg_replslot", "$tempdir/pg_replslot")
  or BAIL_OUT "could not move $pgdata/pg_replslot";
dir_symlink("$tempdir/pg_replslot", "$pgdata/pg_replslot")
  or BAIL_OUT "could not symlink to $pgdata/pg_replslot";

$node->start;

# Test backup of a tablespace using tar format.
# Create a temporary directory in the system location and symlink it
# to our physical temp location.  That way we can use shorter names
# for the tablespace directories, which hopefully won't run afoul of
# the 99 character length limit.
my $sys_tempdir = TestLib::tempdir_short;
my $real_sys_tempdir = "$sys_tempdir/tempdir";
dir_symlink "$tempdir", $real_sys_tempdir;

mkdir "$tempdir/tblspc1";
my $realTsDir    = "$real_sys_tempdir/tblspc1";
$node->safe_psql('postgres',
	"CREATE TABLESPACE tblspc1 LOCATION '$realTsDir';");
$node->safe_psql('postgres',
	    "CREATE TABLE test1 (a int) TABLESPACE tblspc1;"
	  . "INSERT INTO test1 VALUES (1234);");
$node->backup('tarbackup2', backup_options => ['-Ft']);
# empty test1, just so that it's different from the to-be-restored data
$node->safe_psql('postgres', "TRUNCATE TABLE test1;");

# basic checks on the output
my $backupdir = $node->backup_dir . '/tarbackup2';
ok(-f "$backupdir/base.tar",   'backup tar was created');
ok(-f "$backupdir/pg_wal.tar", 'WAL tar was created');
my @tblspc_tars = glob "$backupdir/[0-9]*.tar";
is(scalar(@tblspc_tars), 1, 'one tablespace tar was created');

# Try to verify the tar-format backup by restoring it.
# For this, we use the tar program identified by configure.
SKIP:
{
	my $tar = $ENV{TAR};
	# don't check for a working tar here, to accomodate various odd
	# cases such as AIX. If tar doesn't work the init_from_backup below
	# will fail.
	skip "no tar program available", 1
	  if (!defined $tar || $tar eq '');

	my $node2 = get_new_node('replica');

	# Recover main data directory
	$node2->init_from_backup($node, 'tarbackup2', tar_program => $tar);

	# Recover tablespace into a new directory (not where it was!)
	my $repTsDir     = "$tempdir/tblspc1replica";
	my $realRepTsDir = "$real_sys_tempdir/tblspc1replica";
	mkdir $repTsDir;
	TestLib::system_or_bail($tar, 'xf', $tblspc_tars[0], '-C', $repTsDir);

	# Update tablespace map to point to new directory.
	# XXX Ideally pg_basebackup would handle this.
	$tblspc_tars[0] =~ m|/([0-9]*)\.tar$|;
	my $tblspcoid       = $1;
	my $escapedRepTsDir = $realRepTsDir;
	$escapedRepTsDir =~ s/\\/\\\\/g;
	open my $mapfile, '>', $node2->data_dir . '/tablespace_map';
	print $mapfile "$tblspcoid $escapedRepTsDir\n";
	close $mapfile;

	$node2->start;
	my $result = $node2->safe_psql('postgres', 'SELECT * FROM test1');
	is($result, '1234', "tablespace data restored from tar-format backup");
	$node2->stop;
}

# Create an unlogged table to test that forks other than init are not copied.
$node->safe_psql('postgres',
	'CREATE UNLOGGED TABLE tblspc1_unlogged (id int) TABLESPACE tblspc1;');

my $tblspc1UnloggedPath = $node->safe_psql('postgres',
	q{select pg_relation_filepath('tblspc1_unlogged')});

# Make sure main and init forks exist
ok( -f "$pgdata/${tblspc1UnloggedPath}_init",
	'unlogged init fork in tablespace');
ok(-f "$pgdata/$tblspc1UnloggedPath", 'unlogged main fork in tablespace');

# Create files that look like temporary relations to ensure they are ignored
# in a tablespace.
my $node_dbid = $node->dbid;
my @tempRelationFiles = qw(t_888 t_888888_vm.1);
my $tblSpc1Id = basename(
	dirname(
		dirname(
			$node->safe_psql(
				'postgres', q{select pg_relation_filepath('test1')}))));

foreach my $filename (@tempRelationFiles)
{
	append_to_file(
		"$real_sys_tempdir/tblspc1/$node_dbid/$tblSpc1Id/$postgresOid/$filename",
		'TEMP_RELATION');
}

$node->command_fails(
	[ 'pg_basebackup', '-D', "$tempdir/backup1", '-Fp', '--target-gp-dbid', '-1' ],
	'plain format with tablespaces fails without tablespace mapping and target-gp-dbid as the test server dbid');

$node->command_ok(
	[
		'pg_basebackup', '-D', "$tempdir/backup1", '-Fp',
		'--target-gp-dbid', '1',
		"-T$realTsDir=$tempdir/tbackup/tblspc1",
	],
	'plain format with tablespaces succeeds with tablespace mapping');
ok(-d "$tempdir/tbackup/tblspc1/1", 'tablespace was relocated');

# This symlink check is not supported on Windows as -l
# doesn't work with junctions
SKIP:
{
	skip "symlink check not implemented on Windows", 1
	  if ($windows_os);
	opendir(my $dh, "$pgdata/pg_tblspc") or die;
	ok( (   grep {
				-l "$tempdir/backup1/pg_tblspc/$_"
				  and readlink "$tempdir/backup1/pg_tblspc/$_" eq
				  "$tempdir/tbackup/tblspc1/1"
			} readdir($dh)),
		"tablespace symlink was updated");
	closedir $dh;
}

# Group access should be enabled on all backup files
SKIP:
{
	skip "unix-style permissions not supported on Windows", 1
	  if ($windows_os);

	ok(check_mode_recursive("$tempdir/backup1", 0750, 0640),
		"check backup dir permissions");
}

# Unlogged relation forks other than init should not be copied
my ($tblspc1UnloggedBackupPath) =
  $tblspc1UnloggedPath =~ /[^\/]*\/[^\/]*\/[^\/]*$/g;

ok(-f "$tempdir/tbackup/tblspc1/1/${tblspc1UnloggedBackupPath}_init",
	'unlogged init fork in tablespace backup');
ok(!-f "$tempdir/tbackup/tblspc1/1/$tblspc1UnloggedBackupPath",
	'unlogged main fork not in tablespace backup');

# Temp relations should not be copied.
foreach my $filename (@tempRelationFiles)
{
	ok(!-f "$tempdir/tbackup/tblspc1/1/$tblSpc1Id/$postgresOid/$filename",
		"[tblspc1]/$postgresOid/$filename not copied");

	# Also remove temp relation files or tablespace drop will fail.
	my $filepath =
	  "$real_sys_tempdir/tblspc1/$node_dbid/$tblSpc1Id/$postgresOid/$filename";

	unlink($filepath)
	  or BAIL_OUT("unable to unlink $filepath");
}

ok( -d "$tempdir/backup1/pg_replslot",
	'pg_replslot symlink copied as directory');
rmtree("$tempdir/backup1");

mkdir "$tempdir/tbl=spc2";
$realTsDir = "$real_sys_tempdir/tbl=spc2";
$node->safe_psql('postgres', "DROP TABLE test1;");
$node->safe_psql('postgres', "DROP TABLE tblspc1_unlogged;");
$node->safe_psql('postgres', "DROP TABLESPACE tblspc1;");
$node->safe_psql('postgres',
	"CREATE TABLESPACE tblspc2 LOCATION '$realTsDir';");
$realTsDir =~ s/=/\\=/;
$node->command_ok(
	[
		'pg_basebackup', '-D',
		"$tempdir/backup3", '--target-gp-dbid', '123', '-Fp',
		"-T$realTsDir=$tempdir/tbackup/tbl\\=spc2",
	],
	'mapping tablespace with = sign in path');
ok(-d "$tempdir/tbackup/tbl=spc2", 'tablespace with = sign was relocated');
$node->safe_psql('postgres', "DROP TABLESPACE tblspc2;");
rmtree("$tempdir/backup3");

mkdir "$tempdir/$superlongname";
$realTsDir = "$real_sys_tempdir/$superlongname";
$node->safe_psql('postgres',
	"CREATE TABLESPACE tblspc3 LOCATION '$realTsDir';");
#	skip test since gpdb doesn't support tar file for output
#$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/tarbackup_l3", '--target-gp-dbid', '123', '-Ft' ],
#	'pg_basebackup tar with long symlink target');
$node->safe_psql('postgres', "DROP TABLESPACE tblspc3;");
rmtree("$tempdir/tarbackup_l3");

$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/backupR", '--target-gp-dbid', '123', '-R' ],
	'pg_basebackup -R runs');
ok(-f "$tempdir/backupR/postgresql.auto.conf", 'postgresql.auto.conf exists');
ok(-f "$tempdir/backupR/standby.signal",       'standby.signal was created');
my $recovery_conf = slurp_file "$tempdir/backupR/postgresql.auto.conf";
rmtree("$tempdir/backupR");

my $port = $node->port;
like(
	$recovery_conf,
	qr/^primary_conninfo = '.*port=$port.*'\n/m,
	'postgresql.auto.conf sets primary_conninfo');

$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxd", '--target-gp-dbid', '123' ],
	'pg_basebackup runs in default xlog mode');
ok(grep(/^[0-9A-F]{24}$/, slurp_dir("$tempdir/backupxd/pg_wal")),
	'WAL files copied');
rmtree("$tempdir/backupxd");

$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxf", '--target-gp-dbid', '123', '-X', 'fetch' ],
	'pg_basebackup -X fetch runs');
ok(grep(/^[0-9A-F]{24}$/, slurp_dir("$tempdir/backupxf/pg_wal")),
	'WAL files copied');
rmtree("$tempdir/backupxf");
$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxs", '--target-gp-dbid', '123', '-X', 'stream' ],
	'pg_basebackup -X stream runs');
ok(grep(/^[0-9A-F]{24}$/, slurp_dir("$tempdir/backupxs/pg_wal")),
	'WAL files copied');
rmtree("$tempdir/backupxs");
$node->command_ok(
	[ 'pg_basebackup', '-D', "$tempdir/backupxst", '--target-gp-dbid', '123', '-X', 'stream', '-Ft' ],
	'pg_basebackup -X stream runs in tar mode');
ok(-f "$tempdir/backupxst/pg_wal.tar", "tar file was created");
rmtree("$tempdir/backupxst");
$node->command_ok(
	[
		'pg_basebackup',         '-D',
		"$tempdir/backupnoslot", '-X',
		'stream',                '--no-slot',
		'--target-gp-dbid',     '123',
	],
	'pg_basebackup -X stream runs with --no-slot');
rmtree("$tempdir/backupnoslot");

$node->command_fails(
	[
		'pg_basebackup',             '-D',
		"$tempdir/backupxs_sl_fail", '-X',
		'stream',                    '-S',
		'slot0',
		'--target-gp-dbid',         '123',
	],
	'pg_basebackup fails with nonexistent replication slot');

$node->command_fails(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backupxs_slot", '-C' ],
	'pg_basebackup -C fails without slot name');

$node->command_fails(
	[
		'pg_basebackup',          '-D',
		"$tempdir/backupxs_slot", '-C',
		'-S',                     'slot0',
		'--target-gp-dbid',       '123',
		'--no-slot'
	],
	'pg_basebackup fails with -C -S --no-slot');

$node->command_ok(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backupxs_slot", '-C', '-S', 'slot0' ],
	'pg_basebackup -C runs');
rmtree("$tempdir/backupxs_slot");

is( $node->safe_psql(
		'postgres',
		q{SELECT slot_name FROM pg_replication_slots WHERE slot_name = 'slot0'}
	),
	'slot0',
	'replication slot was created');
isnt(
	$node->safe_psql(
		'postgres',
		q{SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot0'}
	),
	'',
	'restart LSN of new slot is not null');

$node->command_fails(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backupxs_slot1", '-v', '-C', '-S', 'slot0' ],
	'pg_basebackup fails with -C -S and a previously existing slot');

$node->safe_psql('postgres',
	q{SELECT * FROM pg_create_physical_replication_slot('slot1')});
my $lsn = $node->safe_psql('postgres',
	q{SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot1'}
);
is($lsn, '', 'restart LSN of new slot is null');
$node->command_fails(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/fail", '-S', 'slot1', '-X', 'none' ],
	'pg_basebackup with replication slot fails without WAL streaming');
$node->command_ok(
	[
		'pg_basebackup', '-D', "$tempdir/backupxs_sl", '--target-gp-dbid', '123', '-X',
		'stream',        '-S', 'slot1'
	],
	'pg_basebackup -X stream with replication slot runs');
$lsn = $node->safe_psql('postgres',
	q{SELECT restart_lsn FROM pg_replication_slots WHERE slot_name = 'slot1'}
);
like($lsn, qr!^0/[0-9A-Z]{7,8}$!, 'restart LSN of slot has advanced');
rmtree("$tempdir/backupxs_sl");

$node->command_ok(
	[
		'pg_basebackup', '-D', "$tempdir/backupxs_sl_R", '-X',
		'stream',        '-S', 'slot1',                  '-R',
		'--target-gp-dbid', '123'
	],
	'pg_basebackup with replication slot and -R runs');
like(
	slurp_file("$tempdir/backupxs_sl_R/postgresql.auto.conf"),
	qr/^primary_slot_name = 'slot1'\n/m,
	'recovery conf file sets primary_slot_name');

my $checksum = $node->safe_psql('postgres', 'SHOW data_checksums;');
is($checksum, 'on', 'checksums are enabled');
rmtree("$tempdir/backupxs_sl_R");

# create tables to corrupt and get their relfilenodes
my $file_corrupt1 = $node->safe_psql('postgres',
	q{CREATE TABLE corrupt1 AS SELECT a FROM generate_series(1,10000) AS a; ALTER TABLE corrupt1 SET (autovacuum_enabled=false); SELECT pg_relation_filepath('corrupt1')}
);
my $file_corrupt2 = $node->safe_psql('postgres',
	q{CREATE TABLE corrupt2 AS SELECT b FROM generate_series(1,2) AS b; ALTER TABLE corrupt2 SET (autovacuum_enabled=false); SELECT pg_relation_filepath('corrupt2')}
);

# get block size for corruption steps
my $block_size = $node->safe_psql('postgres', 'SHOW block_size;');

# induce corruption
$node->stop;
$node->corrupt_page_checksum($file_corrupt1, 0);
$node->start;

$node->command_checks_all(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backup_corrupt" ],
	1,
	[qr{^$}],
	[qr/^WARNING.*checksum verification failed/s],
	'pg_basebackup reports checksum mismatch');
rmtree("$tempdir/backup_corrupt");

# induce further corruption in 5 more blocks
$node->stop;
for my $i (1 .. 5)
{
	$node->corrupt_page_checksum($file_corrupt1, $i * $block_size);
}
$node->start;

$node->command_checks_all(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backup_corrupt2" ],
	1,
	[qr{^$}],
	[qr/^WARNING.*further.*failures.*will.not.be.reported/s],
	'pg_basebackup does not report more than 5 checksum mismatches');
rmtree("$tempdir/backup_corrupt2");

# induce corruption in a second file
$node->stop;
$node->corrupt_page_checksum($file_corrupt2, 0);
$node->start;

$node->command_checks_all(
	[ 'pg_basebackup', '--target-gp-dbid', '123', '-D', "$tempdir/backup_corrupt3" ],
	1,
	[qr{^$}],
	[qr/^WARNING.*7 total checksum verification failures/s],
	'pg_basebackup correctly report the total number of checksum mismatches');
rmtree("$tempdir/backup_corrupt3");

# do not verify checksums, should return ok
$node->command_ok(
	[
		'pg_basebackup', '--target-gp-dbid', '123',            '-D',
		"$tempdir/backup_corrupt4", '--no-verify-checksums'
	],
	'pg_basebackup with -k does not report checksum mismatch');
rmtree("$tempdir/backup_corrupt4");

$node->safe_psql('postgres', "DROP TABLE corrupt1;");
$node->safe_psql('postgres', "DROP TABLE corrupt2;");

# Some additional GPDB tests
my $twenty_characters = '11111111112222222222';
my $longer_tempdir = "$tempdir/some_long_directory_path_$twenty_characters$twenty_characters$twenty_characters$twenty_characters$twenty_characters";
my $some_backup_dir = "$tempdir/backup_dir";
my $some_other_backup_dir = "$tempdir/other_backup_dir";

mkdir "$longer_tempdir";
mkdir "$some_backup_dir";
$node->psql('postgres', "CREATE TABLESPACE too_long_tablespace LOCATION '$longer_tempdir';");
$node->command_checks_all(
	[ 'pg_basebackup', '-D', "$some_backup_dir", '--target-gp-dbid', '99'],
	1,
	[qr{^$}],
	[qr/symbolic link ".*" target is too long and will not be added to the backup/],
	'basebackup with a tablespace that has a very long location should error out with target is too long.');

mkdir "$some_other_backup_dir";
$node->command_checks_all(
	['pg_basebackup', '-D', "$some_other_backup_dir", '--target-gp-dbid', '99'],
	1,
	[qr{^$}],
	[qr/The symbolic link with target ".*" is too long. Symlink targets with length greater than 100 characters would be truncated./],
	'basebackup with a tablespace that has a very long location should error out link not added to the backup.');

$node->command_checks_all(
	['ls', "$some_other_backup_dir/pg_tblspc/*"],
	2,
	[qr{^$}],
	[qr/No such file/],
	'tablespace directory should be empty');

$node->psql('postgres', "DROP TABLESPACE too_long_tablespace;");

#
# GPDB: Exclude some files with the --exclude-from option
#

my $exclude_tempdir = "$tempdir/backup_exclude";
my $excludelist = "$tempdir/exclude.list";

mkdir "$exclude_tempdir";
mkdir "$pgdata/exclude";

open EXCLUDELIST, ">$excludelist";

# Put a large amount of non-exist patterns in the exclude-from file,
# the pattern matching is efficient enough to handle them.
for my $i (1..1000000) {
	print EXCLUDELIST "./exclude/non_exist.$i\n";
}

# Create some files to exclude
for my $i (1..1000) {
	print EXCLUDELIST "./exclude/$i\n";

	open FILE, ">$pgdata/exclude/$i";
	close FILE;
}

# Below file should not be excluded
open FILE, ">$pgdata/exclude/keep";
close FILE;

close EXCLUDELIST;

$node->command_ok(
	[	'pg_basebackup',
		'-D', "$exclude_tempdir",
		'--target-gp-dbid', '123',
		'--exclude-from', "$excludelist" ],
	'pg_basebackup runs with exclude-from file');
ok(! -f "$exclude_tempdir/exclude/0", 'excluded files were not created');
ok(-f "$exclude_tempdir/exclude/keep", 'other files were created');

# GPDB: Exclude gpbackup default directory
my $gpbackup_test_dir = "$tempdir/gpbackup_test_dir";
mkdir "$pgdata/backups";
append_to_file("$pgdata/backups/random_backup_file", "some random backup data");

$node->command_ok([ 'pg_basebackup', '-D', $gpbackup_test_dir, '--target-gp-dbid', '123' ],
	'pg_basebackup does not copy over \'backups/\' directory created by gpbackup');

ok(! -d "$gpbackup_test_dir/backups", 'gpbackup default backup directory should be excluded');
rmtree($gpbackup_test_dir);

#GPDB: write config files only
mkdir("$tempdir/backup");

$node->command_fails([ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123',
	                   '--write-conf-files-only', '--create-slot', '--slot', "wal_replication_slot"],
	                  'pg_basebackup --write-conf-files-only fails with --create_slot');

$node->command_fails([ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123',
	                   '--write-conf-files-only', '--write-recovery-conf' ],
	                   'pg_basebackup --write-conf-files-only fails with --write-recovery-conf');

$node->command_ok([ 'pg_basebackup', '-D', "$tempdir/backup", '--target-gp-dbid', '123', '--write-conf-files-only' ],
	'pg_basebackup runs with write-conf-files-only');
ok(-f "$tempdir/backup/internal.auto.conf", 'internal.auto.conf was created');
ok(-f "$tempdir/backup/postgresql.auto.conf", 'postgresql.auto.conf was created');
ok(-f "$tempdir/backup/standby.signal",       'standby.signal was created');
rmtree("$tempdir/backup");
