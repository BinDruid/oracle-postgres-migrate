class Validator:
    def __init__(self):
        self.null_conditions = None
        self.type_conditions = None

    def validate_new_record(self, row, oracle_types):
        self._check_not_null(row)
        self._replace_null_string(row)
        self._check_boolean(row, oracle_types)
        self._check_custom_rules(row, oracle_types)

    def _get_fields_of_type(self, field_class):
        return [
            field
            for field, field_type in self.type_conditions.items()
            if field_type == field_class
        ]
    def _replace_null_string(self, row):
        for field, value in row.items():
            if isinstance(value, str):
                row[field] = value.replace("\x00", "")
                
    def _get_none_null_fields(self):
        return [
            field
            for field, null_constraint in self.null_conditions.items()
            if null_constraint == "NO"
        ]

    def _check_boolean(self, row, oracle_types):
        for field in self._get_fields_of_type("boolean"):
            if oracle_types[field] == "number":
                row[field] = bool(row[field])

    def _check_not_null(self, row):
        for field in self._get_none_null_fields():
            if (
                row[field] is None
                and self.type_conditions[field] == "character varying"
            ):
                row[field] = ""

    def _check_custom_rules(self, row, oracle_types):
        pass
