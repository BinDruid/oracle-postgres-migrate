class Validator:
    null_conditions = {}
    type_conditions = {}

    def validate_new_record(self, row, context):
        self._check_not_null(row)
        self._check_boolean(row, context)

    def _get_fields_of_type(self, keyword):
        return [
            field
            for field, field_type in self.type_conditions.items()
            if field_type == keyword
        ]

    def _get_not_null_fields(self):
        return [
            field
            for field, field_constraint in self.null_conditions.items()
            if field_constraint == "No"
        ]

    def _check_boolean(self, row, context):
        for field in self._get_fields_of_type("bool"):
            if context[field] != "bool":
                row[field] = bool(row[field])

    def _check_not_null(self, row):
        for field in self._get_not_null_fields():
            if row[field] is None:
                row[field] = ""
