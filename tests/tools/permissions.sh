# describe_permissions <user> <file>
# describes what permissions the user <user> has when accessing <file>
describe_permissions() {
	local user=$1
	local file=$2
	sudo -nu "$user" python3 - "$file" <<'END_OF_SCRIPT'
tests = [
	('mkdir',     'os.mkdir(sys.argv[1] + "/x")'),
	('rmdir',     'os.rmdir(sys.argv[1] + "/x")'),
	('list',      'os.listdir(sys.argv[1])'),
	('read',      'open(sys.argv[1], "r").read()'),
	('write',     'open(sys.argv[1], "a").write("a")'),
	('readlink',  'os.readlink(sys.argv[1])'),
	('stat',      'os.stat(sys.argv[1])'),
	('utimenow',  'os.utime(sys.argv[1])'),
	('utime',     'os.utime(sys.argv[1], (123, 456))'),
	('listxattr', 'os.listxattr(sys.argv[1])'),
	('R',         'if not os.access(sys.argv[1], os.R_OK): raise OSError'),
	('W',         'if not os.access(sys.argv[1], os.W_OK): raise OSError'),
	('X',         'if not os.access(sys.argv[1], os.X_OK): raise OSError'),
	('RX',        'if not os.access(sys.argv[1], os.R_OK | os.X_OK): raise OSError'),
	('WX',        'if not os.access(sys.argv[1], os.W_OK | os.X_OK): raise OSError'),
	('RW',        'if not os.access(sys.argv[1], os.R_OK | os.W_OK): raise OSError'),
	('RWX',       'if not os.access(sys.argv[1], os.R_OK | os.W_OK | os.X_OK): raise OSError')]
import os, sys
for (name, code) in tests:
	try:
		exec(code)
		sys.stdout.write(' ' + name)
	except EnvironmentError: # OSError or IOError
		pass
print("")
END_OF_SCRIPT
}
