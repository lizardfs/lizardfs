assert_program_installed nfs4_setfacl
assert_program_installed nfs4_getfacl

CHUNKSERVERS=3 \
	USE_RAMDISK=YES \
	setup_local_empty_lizardfs info

cd ${info[mount0]}

mkdir -p dir1/dir2

FILE_SIZE=1234567 file-generate file1
FILE_SIZE=2345678 file-generate dir1/file2
FILE_SIZE=3456789 file-generate dir1/dir2/file3

chmod 755 dir1
chmod 755 dir1/dir2

chmod 664 file1
chmod 777 dir1/file2
chmod 600 dir1/dir2/file3

ls -l

# Step 1: default ACLs for files and directories

# Positives for file1
nfs4_getfacl file1 | grep OWNER | grep ':rwa'
nfs4_getfacl file1 | grep GROUP | grep ':rwa'
nfs4_getfacl file1 | grep EVERYONE | grep ':r'

# Negatives for file1
nfs4_getfacl file1 | grep EVERYONE | grep -v ':rw'

# Positives for file2
nfs4_getfacl dir1/file2 | grep EVERYONE | grep ':rwax'

# Negatives for file2
nfs4_getfacl dir1/file2 | grep -v OWNER
nfs4_getfacl dir1/file2 | grep -v GROUP

# Positives for file3
nfs4_getfacl dir1/dir2/file3 | grep OWNER | grep ':rwa'

# Negatives for file3
nfs4_getfacl dir1/dir2/file3 | grep -v GROUP
nfs4_getfacl dir1/dir2/file3 | grep -v EVERYONE

# Directories
nfs4_getfacl dir1 | grep OWNER | grep ':rwaDx'
nfs4_getfacl dir1 | grep EVERYONE | grep ':rx'
nfs4_getfacl dir1/dir2 | grep OWNER | grep ':rwaDx'
nfs4_getfacl dir1/dir2 | grep EVERYONE | grep ':rx'

# Step 2: custom ACLs for files

nfs4_setfacl -a A::lizardfstest_1:rwxcCa file1
nfs4_setfacl -a A::lizardfstest_2:rwcCa file1
nfs4_setfacl -a D::lizardfstest_3:rC file1

nfs4_getfacl file1 | grep 'D::lizardfstest_3:rC'
nfs4_getfacl file1 | grep 'A::lizardfstest_2:rwacC'
nfs4_getfacl file1 | grep 'A::lizardfstest_1:rwaxcC'

assert_equals 3 $(nfs4_getfacl file1 | grep lizardfstest_ | wc -l)

# Step 3: custom ACLs for directories

nfs4_setfacl -a D::lizardfstest_5:rwCD dir1
nfs4_setfacl -a A::lizardfstest_6:D dir1

nfs4_setfacl -a A::lizardfstest_6:D dir1/dir2
nfs4_setfacl -a D::lizardfstest_7:Dx dir1/dir2

nfs4_getfacl dir1 | grep OWNER | grep ':rwaDx'
nfs4_getfacl dir1 | grep EVERYONE | grep ':rx'
nfs4_getfacl dir1/dir2 | grep OWNER | grep ':rwaDx'
nfs4_getfacl dir1/dir2 | grep EVERYONE | grep ':rx'

nfs4_getfacl dir1 | grep 'A::lizardfstest_6:D'
nfs4_getfacl dir1 | grep 'D::lizardfstest_5:rwDC'

nfs4_getfacl dir1/dir2 | grep 'D::lizardfstest_7:Dx'
nfs4_getfacl dir1/dir2 | grep 'A::lizardfstest_6:D'
