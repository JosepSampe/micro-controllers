from pyparsing import Word, alphas, Literal, Group, Combine, opAssoc, alphanums, OneOrMore
from pyparsing import Regex, operatorPrecedence, oneOf, ZeroOrMore, nums, Optional, delimitedList

def parse(input_string):

    """ Support words to construct the grammar """
    WORD = Word(alphanums+"_-.^[]")
    SLASH = Literal("/")
    LBRKT = Literal("{")
    RBRKT = Literal("}")
    COLON = Literal(":")
    MARK = Literal("'")
    COMMA = Literal(",")
    
    WHEN = Literal("WHEN").suppress()
    FOR = Literal("FOR").suppress()
    MOVE = Literal("MOVE").suppress()
    COPY = Literal("COPY").suppress()
    GROUP = Literal("GROUP").suppress()
    DELETE = Literal("DELETE").suppress()
    DO = Literal("DO").suppress()
    AS = Literal("AS").suppress()
    ON = Literal("ON").suppress()
    BY = Literal("BY").suppress()
    
    condition = oneOf("AND OR")
    action = oneOf("SET DELETE")("action")
    operands = oneOf("< > == != <= >=")


    """ 
        Active Storage Orchestation
    """
    
    """ Single object format in Swift """
    obj = Combine(WORD + SLASH + WORD + Optional(SLASH + WORD))
    """ List of objects delimited by comma """
    objects = Group(delimitedList(obj))

    filters = map(lambda x: "".join(x), {'USER_NAME', 'ROLES'}) # Object Metadata
    filter_list = oneOf(filters)("filter")
    
    storlets = map(lambda x: "".join(x), {'COMPRESSION', 'FILTER_CONTENT', 'TRANSCODER', 'GREP'}) # Storlets
    storlets_list = oneOf(storlets)("storlet")
    json = Combine(MARK + WORD + MARK + COLON + MARK + WORD + MARK + Optional(COMMA))
    metadata = Combine(LBRKT + OneOrMore(json) + RBRKT)("metadata")
    action = Group(action + storlets_list + Optional(BY + filter_list + metadata) + Optional(metadata))
    actions = Group(delimitedList(action))
        
    """ Final rule structure """
    rule_parse = FOR + objects("objects") + DO + actions("actions")
    """ Parse the rule """
    parsed_rule = rule_parse.parseString(input_string)
    
    return parsed_rule


""" ----------------------------------- TEST DSL PARSER ------------------------------------ """
       
rule_parsed = parse("FOR data/out/sample_3mb.csv, data/sampe3_gb.csv DO SET COMPRESSION BY USER_NAME {'josep':'gz','raul':'zip'}, SET GREP BY ROLES {'admin':'^r'}")

print "Active storage orchestration test:"
for obj in rule_parsed.objects:
    print " Object ID: " + str(obj)

print "\nStorlets applied:"
for action in rule_parsed.actions:
    print " Action: " + action.action
    print " Storlet: " + action.storlet
    print " Filter: " + action.filter
    print " Metadata: " + action.metadata
    print " -------------------------------------"

