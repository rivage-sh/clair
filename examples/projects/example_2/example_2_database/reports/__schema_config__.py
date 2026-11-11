from clair import SchemaDefaults

# Each run makes the reports again from the derived tables, thus a Fail-safe
# copy protects nothing. Each table of this schema is transient. The schema
# takes the warehouse and the role from __database_config__.py.
defaults = SchemaDefaults(transient=True)
