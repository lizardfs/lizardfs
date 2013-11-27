#!/usr/bin/env python3

import sys
import re

# Some configuration goes here
chunk_prefixes = ['', 'old', 'new', 'copy'] # prefixes of chunkid field, eg. copychunkid
oct_fields = ['mode', 'modemask']
hex_fields = ['vershex', 'rver', 'ip', 'crc']
for pfx in chunk_prefixes:
    hex_fields += [pfx + 'chunkid', pfx + 'chunkversion']
fields_with_dictionary = ['type', 'gmode', 'smode', 'status', 'nodetype']

class Types:
    int_dec = 1
    int_hex = 2
    int_oct = 3
    string = 4
    blob = 5

    @staticmethod
    def is_number(type):
        return type in [Types.int_dec, Types.int_hex, Types.int_oct]

    to_wireshark_type = {
        int_dec: 'FT_UINT',
        int_hex: 'FT_UINT',
        int_oct: 'FT_UINT',
        string:  'FT_STRING',
        blob:    'FT_BYTES',
    }

    to_wireshark_base = {
        int_dec: 'BASE_DEC',
        int_hex: 'BASE_HEX',
        int_oct: 'BASE_OCT',
        string:  'BASE_NONE',
        blob:    'BASE_NONE',
    }

    int_length_to_getter = {
        1: 'tvb_get_guint8',
        2: 'tvb_get_ntohs',
        4: 'tvb_get_ntohl',
        8: 'tvb_get_ntoh64',
    }

class PacketDissectionVariant(object):
    def __init__(self, message):
        object.__init__(self)
        self._condition = None
        self.elements = []
        self.finished = False
        extra_size = 4 if message[:4] == 'LIZ_' else 0
        self.position = 8 + extra_size
        self.position_variable = ''
        self.minlength = extra_size
        self.length = extra_size
        self.has_infinite_field = False

    def get_current_position(self):
        return str(self.position) + self.position_variable

    def add_field(self, type, name, length):
        if self.finished:
            raise RuntimeError('Cannot add field {} after a field with unspecified length'.format(name))
        self.elements.append((type, name, self.get_current_position(), length))
        if length is None:
            self.finished = True
            self.length = None
            self.has_infinite_field = True
            return
        try:
            length = int(length)
            self.position += length
            self.minlength += length
            if self.length is not None:
                self.length += length
        except ValueError:
            self.position_variable = self.position_variable + '+' + length
            self.length = None

    def add_int_field(self, type, name, bits):
        assert Types.is_number(type)
        self.add_field(type, name, int(bits) // 8)

    def add_string_field(self, name, length=None):
        self.add_field(Types.string, name, length)

    def add_blob_field(self, name, length=None):
        self.add_field(Types.blob, name, length)

    def add_name_field(self, name):
        length_var = name + '__strlen'
        self.add_field(Types.int_dec, length_var, 1)
        self.add_field(Types.string, name, length_var)

    def add_condition(self, variable, condition):
        assert self._condition is None
        self._condition = (variable, condition)

    @property
    def condition(self):
        if self._condition:
            return self._condition
        elif self.length is not None:
            return ('length', ' == {}'.format(self.length))
        elif self.minlength == 0:
            return ('1', '')
        else:
            return ('length', ' >= {}'.format(self.minlength))

    def get_info(self):
        info_format = ""
        info_args = []
        numvariables = {name for (type, name, _, _) in self.elements if Types.is_number(type)}
        # Find chunks and print them at the beginning in the format like this:
        # chunk_000000000000026F_00000001
        for prefix in ['', 'old', 'new', 'copy']:
            with_version = [prefix + 'chunk' + name for name in ['id', 'version']]
            if all(name in numvariables for name in with_version):
                info_format += ' {}chunk_%016lX_%08X'.format(prefix + ':' if prefix else '')
                info_args += [prefix + 'chunkid', prefix + 'chunkversion']
                for var in with_version:
                    numvariables.remove(var)
        # Print all other arguments as numbers
        for (type, name, pos, length) in self.elements:
            if type == Types.blob or '__strlen' in name:
                pass
            elif type == Types.string and length is not None:
                info_format += r' {}:\"%.*s\"'.format(name)
                info_args += ['(int)' + str(length), name]
            elif name not in numvariables:
                pass
            elif name in fields_with_dictionary:
                info_format += ' {}:%s'.format(name)
                info_args.append('val_to_str({0}, dictionary_{0}, "UNKNOWN(%d)")'.format(name))
            else:
                int_format = '0x%{}X' if name in hex_fields else '0o%03{}o' if name in oct_fields else '%{}u'
                int_format = int_format.format('"G_GINT64_MODIFIER"')
                info_format += ' {}:{}'.format(name, int_format)
                info_args.append('(guint64){}'.format(name))
        return (info_format, info_args)

    def get_element(self, name):
        for (type, element_name, start, length) in self.elements:
            if element_name == name:
                return (type, name, start, length)
        raise KeyError('No such element: "{}"'.format(name))

    def print_method(self, funcname):
        # Create header
        print('static void {}('.format(funcname))
        print('        tvbuff_t *tvb, guint32 length,')
        print('        packet_info *pinfo, proto_tree *tree) {')
        # Declare local variables
        for (type, name, start, length) in self.elements:
            if Types.is_number(type):
                print('    guint{} {};'.format(length * 8, name))
            elif type == Types.string and length is not None:
                print('    const char *{};'.format(name))
        # Verify packet length
        if self.length is not None:
            condition = 'length == {}'.format(self.length)
        elif self.minlength == 0:
            condition = '1'
        else:
            condition = 'length >= {}'.format(self.minlength)
        print('    if (!({})) {{'.format(condition))
        # TODO(msulikowski) Add some flag to the protocol tree indicating this error
        print('        col_append_str(pinfo->cinfo, COL_INFO, ')
        print('                " [ERROR: PACKET LENGTH MISMATCH, EXPECTED {}]");'.format(condition))
        print('        return;')
        print('    }')
        # Read values of variables
        for (type, name, start, length) in self.elements:
            if Types.is_number(type):
                print('    {} = {}(tvb, {});'.format(name, Types.int_length_to_getter[length], start))
            elif type == Types.string and length is not None:
                print('    {} = (const char*)tvb_get_ptr(tvb, {}, {});'.format(name, start, length))
        # Check the length once again if is variable
        if self.length is None and not self.has_infinite_field:
            leng = str(self.position - 8) + self.position_variable
            print('    if (length != (guint32)({})) {{'.format(leng))
            print('        col_append_fstr(pinfo->cinfo, COL_INFO, ')
            print('                " [ERROR: PACKET LENGTH MISMATCH, EXPECTED {}, WHICH IS %lu]",'.format(leng))
            print('                (unsigned long)({}));'.format(leng))
            print('    }')
        # Add info column
        (info_format, info_args) = self.get_info()
        if info_format:
            print('    col_append_fstr(pinfo->cinfo, COL_INFO,')
            print('            "' + info_format + '",')
            print('            ' + ', '.join(info_args) + ');')
        # Add elements to the protocol tree
        print('    if (tree) {')
        for (type, name, start, length) in self.elements:
            if '__strlen' in name:
                continue
            if self.length is None:
                print('        if (length + 8 < (guint32)({}+{})) return;'.format(start, 0 if length is None else length))
            if length is None:
                print('        if (length + 8 > {})'.format(start), end='')
                length = -1
            print('        proto_tree_add_item(tree, hf_lizardfs_{}, tvb, {}, {}, {});'.format(
                    name, start, length, 'ENC_BIG_ENDIAN' if Types.is_number(type) else 'ENC_NA'))
        print('    }')
        # Ged rid of compiler's warnings
        print('    (void)tvb;')
        print('    (void)tree;')
        print('    (void)length;')
        print('}\n')

# Parse input
dissectinfo = {}
field_types = { 'type':Types.int_dec, 'length':Types.int_dec, 'version':Types.int_dec }
int_field_bits = { 'type':32, 'length':32, 'version':32 }
command = None
for line in sys.stdin:
    try:
        tokens = line.split()
        assert len(tokens) > 1
        keyword, *args = tokens
        if keyword == 'Packet':
            assert len(args) == 1
            command = args[0]
            dissectinfo[command] = []
            continue
        assert keyword == 'DissectAs'
        assert command is not None
        variant = PacketDissectionVariant(command)
        condition_added = False
        regex = r'([a-z_0-9]+)([!=<>/*%-+()][^:]*)?(?:(?::)(.*))?'
        for arg in args:
            if arg == '-':
                break
            (name, condition, typestr) = re.match(regex, arg).group(1, 2, 3)
            assert condition is not None or type is not None
            # Condition, ie. somthing like version==3 or rver==0x55:8
            if condition:
                variant.add_condition(name, condition)
                condition_added = True
                if typestr is None:
                    # A plain condition, ie. length or version
                    continue
            # 35B is a shortef form of BYTES[35]; epand it
            if re.match(r'^[0-9]+B$', typestr):
                typestr = 'BYTES[' + typestr[:-1] + ']'
            # int fields: 8, 16, 32, 64
            if typestr in ['8', '16', '32', '64']:
                type = Types.int_dec
                if name in oct_fields: type = Types.int_oct
                if name in hex_fields: type = Types.int_hex
                bits = int(typestr)
                variant.add_int_field(type, name, bits)
                if name not in int_field_bits:
                    int_field_bits[name] = bits
                int_field_bits[name] = max(bits, int_field_bits[name])
            # NAME field
            elif typestr == 'NAME':
                type = Types.string
                variant.add_name_field(name)
            # STRING, STRING[32] or STRING[some_variable]
            elif re.match(r'^STRING(\[[0-9a-zA-Z_]+\])?$', typestr):
                type = Types.string
                if '[' in typestr:
                    length = typestr[7:-1]
                else:
                    length = None
                variant.add_string_field(name, length)
            # BYTES, BYTES[32] or BYTES[some_variable]
            elif re.match(r'^BYTES(\[[0-9a-zA-Z_]+\])?$', typestr):
                type = Types.blob
                if '[' in typestr:
                    length = typestr[6:-1]
                else:
                    length = None
                variant.add_blob_field(name, length)
            else:
                raise RuntimeError('Cannot parse type "{}" in {}'.format(typestr, arg))
            if name not in field_types:
                field_types[name] = type
            if not field_types[name] == type:
                raise RuntimeError('Type for {} was {}, but now it is {}'.format(name, field_types[name], type))
        # Default condition for LIZ_* packets is version==0
        if command[:4] == 'LIZ_' and not condition_added:
            variant.add_condition('version', ' == 0')
        dissectinfo[command].append(variant)
    except Exception as ex:
        print('Cannot parse line "{}" for command {}'.format(line[:-1], command), file=sys.stderr)
        import traceback
        print(traceback.format_exc(), file=sys.stderr)
        sys.exit(1)

# Generate includes (to make IDEs happy)
print('#include "config.h"')
for header in ['<glib.h>', '<epan/dissectors/packet-tcp.h>', '<epan/packet.h>', '<epan/packet_info.h>',
        '<epan/prefs.h>', '<epan/tvbuff.h>', '<epan/value_string.h>']:
    print('#include {}'.format(header))
print('#include "includes.h"')

# Generate global variables
print('static dissector_handle_t lizardfs_handle;')
print('static int proto_lizardfs = -1;')
print('static gint ett_lizardfs = -1;')
print('static range_t *tcp_ports_lizardfs = NULL;')
for field in sorted(field_types):
    print('static int hf_lizardfs_{} = -1;'.format(field))
print()

# Generate dictionaries
print('#define LIZARDFS_CONST_TO_NAME_ENTRY(name) {name, #name}')
for field in sorted(fields_with_dictionary):
    print('''
static const value_string dictionary_{0}[] = {{
#   include "dict_{0}-inl.h"
    {{0, NULL}}
}};
'''.format(field))

# Generate dissectors for all messages
for message in sorted(dissectinfo):
    n = 0
    print('/**************    dissections of {}    **************/'.format(message))
    for variant in dissectinfo[message]:
        variant.print_method('dissect_{}_variant_{}'.format(message, n))
        n += 1
    print('static void dissect_{}('.format(message))
    print('        tvbuff_t *tvb, guint32 length, guint32 version,')
    print('        packet_info *pinfo, proto_tree *tree) {')
    if len(dissectinfo[message]) == 0:
        # There is no information available about dissection of this message
        parsed_part_len = 12 if message[:4] == 'LIZ_' else 8
        print('    col_append_str(pinfo->cinfo, COL_INFO, " (info not available)");')
        print('    if (length > {}) {{'.format(parsed_part_len - 8))
        print('        proto_tree_add_item(tree, hf_lizardfs_data, tvb, {}, -1, ENC_NA);'.format(parsed_part_len))
        print('    }')
    else:
        # Get values of any variables needed to choose a variant
        condition_variables = set([variant.condition[0] for variant in dissectinfo[message]])
        for known_variable in ['1', 'length', 'version']:
            condition_variables.discard(known_variable)
        for name in condition_variables:
            (type, _, start, length) = dissectinfo[message][0].get_element(name)
            print('    guint{} {} = {}(tvb, {});'.format(length * 8, name, Types.int_length_to_getter[length], start))
        # Choose the right variant
        n = 0
        conditions = []
        for variant in dissectinfo[message]:
            condition = '({}{})'.format(*variant.condition)
            conditions.append(condition)
            print('    if {} {{'.format(condition))
            print('        return dissect_{}_variant_{}(tvb, length, pinfo, tree);'.format(message, n))
            print('    }')
            n += 1
        # TODO(msulikowski) Add some flag to the protocol tree indicating this error
        print('    col_append_str(pinfo->cinfo, COL_INFO, ')
        print('            " [ERROR: UNKNOWN VARIANT, EXPECTED {}]");'.format(" OR ".join(conditions)))
    print('    (void)version;')
    print('}\n')

# Generate dissector for lizardfs
print('''
static void dissect_lizardfs_message(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree) {
    proto_item *ti = NULL;
    proto_tree *lizardfs_tree = NULL;
    guint32 type = 0;
    guint32 length = 0;
    guint32 version = 0;

    type = tvb_get_ntohl(tvb, 0);
    length = tvb_get_ntohl(tvb, 4);
    if (type >= 1000 && type <= 2000) {
        version = tvb_get_ntohl(tvb, 8);
    }

    col_set_str(pinfo->cinfo, COL_PROTOCOL, "LizardFS");
    col_clear(pinfo->cinfo, COL_INFO);
    col_add_fstr(pinfo->cinfo, COL_INFO, "%-30s",
                 val_to_str(type, dictionary_type, "UNKNOWN(0x%02x)"));

    if (tree) {
        ti = proto_tree_add_item(tree, proto_lizardfs, tvb, 0, -1, ENC_NA);
        proto_item_append_text(ti, ", %s",
                val_to_str(type, dictionary_type, "UNKNOWN(0x%02x)"));

        lizardfs_tree = proto_item_add_subtree(ti, ett_lizardfs);
        proto_tree_add_item(lizardfs_tree, hf_lizardfs_type, tvb, 0, 4, ENC_BIG_ENDIAN);
        proto_tree_add_item(lizardfs_tree, hf_lizardfs_length, tvb, 4, 4, ENC_BIG_ENDIAN);
        if (type >= 1000 && type <= 2000) {
            proto_tree_add_item(lizardfs_tree, hf_lizardfs_version, tvb, 8, 4, ENC_BIG_ENDIAN);
        }
    }

    switch(type) {
''')
for message in sorted(dissectinfo):
    print('        case {}:'.format(message))
    print('            dissect_{}(tvb, length, version, pinfo, lizardfs_tree);'.format(message))
    print('            break;')
print('    }')
print('}')# Generate function registering the protocol
print('''
static guint lizardfs_get_message_length(packet_info *pinfo, tvbuff_t *tvb, int offset) {
    (void)pinfo;
    (void)tvb;
    return (guint)(tvb_get_ntohl(tvb, offset + 4) + 8);
}

static void dissect_lizardfs(tvbuff_t *tvb, packet_info *pinfo, proto_tree *tree) {
    col_clear(pinfo->cinfo, COL_INFO);
    tcp_dissect_pdus(tvb, pinfo, tree, TRUE, 8, lizardfs_get_message_length, dissect_lizardfs_message);
}

static void register_tcp_port(guint32 port) {
    if (port != 0) {
        dissector_add_uint("tcp.port", port, lizardfs_handle);
    }
}

static void unregister_tcp_port(guint32 port)
{
    if (port != 0) {
        dissector_delete_uint("tcp.port", port, lizardfs_handle);
    }
}

void proto_reg_handoff_lizardfs(void) {
    static gboolean lizardfs_initialized = FALSE;
    static range_t *tcp_ports_lizardfs_copy  = NULL;

    if (!lizardfs_initialized) {
        lizardfs_handle = create_dissector_handle(dissect_lizardfs, proto_lizardfs);
        lizardfs_initialized = TRUE;
    } else {
        if (tcp_ports_lizardfs_copy != NULL) {
            range_foreach(tcp_ports_lizardfs_copy, unregister_tcp_port);
            g_free(tcp_ports_lizardfs_copy);
        }
    }
    tcp_ports_lizardfs_copy = range_copy(tcp_ports_lizardfs);
    range_foreach(tcp_ports_lizardfs_copy, register_tcp_port);
}

void proto_register_lizardfs(void) {
    module_t *lizardfs_module = NULL;

    /* Subtree */
    static gint *ett[] = { &ett_lizardfs };

    /* Fields */
    static hf_register_info hf[] = {''')
for field in sorted(field_types):
    type = field_types[field]
    typestr = Types.to_wireshark_type[type]
    if field in int_field_bits:
        typestr += str(int_field_bits[field])
    basestr = Types.to_wireshark_base[type]
    dictstr = "VALS(dictionary_{})".format(field) if field in fields_with_dictionary else "NULL"
    print('        {{&hf_lizardfs_{}, {{'.format(field))
    print('            "{0}", "lizardfs.{0}",'.format(field))
    print('            {}, {}, {}, 0, NULL, HFILL}}'.format(typestr, basestr, dictstr))
    print('        },')
print('''};
    proto_lizardfs = proto_register_protocol("LizardFS Protocol", "LizardFS", "lizardfs");
    proto_register_field_array(proto_lizardfs, hf, array_length(hf));
    proto_register_subtree_array(ett, array_length(ett));
    range_convert_str(&tcp_ports_lizardfs, "9419-9422",  65535);
    lizardfs_module = prefs_register_protocol(proto_lizardfs, proto_reg_handoff_lizardfs);
    prefs_register_range_preference(lizardfs_module, "tcp_ports",
				 "LizardFS TCP Ports",
				 "The TCP ports for the LizardFS Protocol",
				 &tcp_ports_lizardfs, 65535);
}''')
