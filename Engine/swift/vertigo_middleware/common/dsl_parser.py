from pyparsing import Word, Literal, Group, Combine, alphanums, OneOrMore, oneOf, nums, Optional, delimitedList

def parse(input_string):

    """ Support words to construct the grammar """
    WORD = Word(alphanums+"_-.^[]")
    NUMBER = Word(nums)
    SLASH = Literal("/")
    LBRKT = Literal("{")
    RBRKT = Literal("}")
    COLON = Literal(":")
    MARK = Literal("'")
    COMMA = Literal(",")
    
    WHEN = Literal("WHEN").suppress()
    FOR = Literal("FOR").suppress()
    
    MOVECOPY = oneOf("MOVE COPY")("action")
    GROUP = Literal("GROUP")("action")
    DELETE = Literal("DELETE")("action")
    
    DO = Literal("DO").suppress()
    AS = Literal("AS").suppress()
    ON = Literal("ON").suppress()
    BY = Literal("BY").suppress()
    TO = Literal("TO").suppress()
    
    condition = oneOf("AND OR")
    action = oneOf("SET DELETE")("action")
    operand = oneOf("< > == != <= >=")


    """ Single object format in Swift """
    obj = Combine(WORD + SLASH + WORD + Optional(SLASH + WORD))
    """ List of objects delimited by comma """
    objects = Group(delimitedList(obj))("objects")

    filters = map(lambda x: "".join(x), {'USER_NAME', 'ROLES'}) # Object Metadata
    filter_list = oneOf(filters)("filter")
    
    storlets = map(lambda x: "".join(x), {'COMPRESSION', 'FILTER_CONTENT', 'TRANSCODER', 'GREP'}) # Storlets
    storlets_list = oneOf(storlets)("storlet")
    json = Combine(MARK + WORD + MARK + COLON + MARK + WORD + MARK + Optional(COMMA))
    metadata = Combine(LBRKT + OneOrMore(json) + RBRKT)("metadata")
    action = Group(action + storlets_list + Optional(BY + filter_list + metadata) + Optional(metadata))
    actions = Group(delimitedList(action))
    condition = WORD + operand + NUMBER
    
    """ ACTIVE STORAGE ORCHESTRATION """
    aso = FOR + objects + DO + actions("actions")

    """ GROUPING COMMAND """
    grouping = GROUP + objects + AS + obj("group")
    
    """ MOVE, COPY COMMAND """
    move_copy = MOVECOPY + obj("source") + TO + obj("destination") + Optional(WHEN + condition)("condition")
    
    """ DELETE COMMAND """
    delete = DELETE + objects + Optional(WHEN + condition)("condition")
    
    
    """ FINAL RULE PARSER """
    rule_parser = aso ^ grouping ^ move_copy ^ delete

    """ Parse the rule """
    parsed_rule = rule_parser.parseString(input_string)
    
    return parsed_rule


""" ----------------------------------- TEST DSL PARSER ------------------------------------ """
 
rule_parsed = parse("FOR data/out/sample_3mb.csv, data/sampe3_gb.csv DO SET COMPRESSION BY USER_NAME {'pepe':'gz','david':'zip'}, SET GREP BY ROLES {'admin':'^r'}")
 
print "--> Active storage orchestration test:"
for obj in rule_parsed.objects:
    print " Object ID: " + str(obj)
 
print "\nStorlets applied:"
for action in rule_parsed.actions:
    print " Action: " + action.action
    print " Storlet: " + action.storlet
    print " Filter: " + action.filter
    print " Metadata: " + action.metadata
    print " -------------------------------------"

""" MOVE OBJECT """
rule_parsed = parse("MOVE data/out/sample_3mb.csv TO data/sampe3_gb.csv WHEN gets > 5")
print "\n--> Move test:"
print " Action: " + rule_parsed.action
print " Source: " + rule_parsed.source
print " Destination: " + rule_parsed.destination
print " Condition: " + str(rule_parsed.condition)
 
""" COPY OBJECT """
rule_parsed = parse("COPY data/out/object.csv TO data/out/object_copy.csv.csv")
print "\n--> Copy test:"
print " Action: " + rule_parsed.action
print " Source: " + rule_parsed.source
print " Destination: " + rule_parsed.destination
print " Condition: " + str(rule_parsed.condition)
 
""" DELETE OBJECT """
rule_parsed = parse("DELETE data/4.csv, data/3.csv, data/2.csv WHEN gets > 100")
print "\n--> Delete test:"
print " Action: " + rule_parsed.action
print " Objects: " + str(rule_parsed.objects)
print " Condition: " + str(rule_parsed.condition)

""" GROUPPING OBJECTS """
rule_parsed = parse("GROUP data/4.csv, data/3.csv, data/2.csv, data/1.csv AS data/object_goup_1")
print "\n--> Group test:"
print " Action: " + rule_parsed.action
print " Objects: " + str(rule_parsed.objects)
print " Group: " + str(rule_parsed.group)
