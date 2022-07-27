

class BaseOp:

    def __init__(self, command, required_params, optional_params):
        self.command = command
        self.required_params = required_params
        self.optional_params = optional_params

    def check_parameters(self, parameters):
        # Check for required arguments
        required = set(self.required_params.keys())
        required_missing = required.difference(set(parameters.keys()))
        if required_missing:
            raise KeyError("MissingRequiredParameters", f"{self.command} requires parameters {list(required_missing)}")
        for param_name, param_value in parameters.items():
            if param_name in self.required_params:
                param_type = self.required_params[param_name]
            elif param_name in self.optional_params:
                param_type = self.optional_params[param_name]
            else:
                raise KeyError("BadParameter", f"{param_name} not a required or optional parameter for {self.command}")
            if not isinstance(param_value, param_type):
                raise TypeError("BadType" f"{param_value} has type {type(param_value)} not {param_type}")

    def do_op(self, df, hed_schema=None, sidecar=None):
        return df
