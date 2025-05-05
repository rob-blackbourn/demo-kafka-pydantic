import json
import importlib

from pydantic import BaseModel, ValidationInfo


def serialize_model(model: BaseModel) -> dict:
    """Serialize a Pydantic model to a dictionary.

    Two fields are added to the root level of the dictionary:
        - `__module__`: The module where the model is defined.
        - `__qualname__`: The qualified name of the model class.

    This allows the model to be deserialized later by importing the module
    and using the qualified name to get the class.

    This can be used in combination with `validate_model` in the following way:

    ```python
    class Update[T: BaseModel](BaseModel):
        model: Annotated[
            T,
            PlainSerializer(serialize_model),
            PlainValidator(validate_model)
        ]
    ```

    Args:
        model (BaseModel): The model to serialize.

    Returns:
        dict: A python dictionary representation of the model where the
            keys are strings and the values are JSON serializable. Metadata
            about the model is included in the dictionary.
    """
    dct = model.model_dump(mode='json')
    dct['__module__'] = model.__class__.__module__
    dct['__qualname__'] = model.__class__.__qualname__
    return dct


def _deserialize_model(dct: dict) -> BaseModel:
    """This function deserializes a dictionary into a Pydantic model.

    It uses the `__module__` and `__qualname__` keys in the dictionary to
    import the module and get the class.

    Args:
        dct (dict): The dictionary to deserialize.

    Returns:
        BaseModel: The deserialized Pydantic model.
    """
    module = dct.pop('__module__')
    qualname = dct.pop('__qualname__')
    cls = getattr(importlib.import_module(module), qualname)
    model = cls.model_validate(dct)
    return model


def serialize_model_to_json(model: BaseModel) -> str:
    """Serialize a Pydantic model to a JSON string.

    The serialization will inject metadata whenever it encounters a pydantic
    `BaseModel` allowing deserialization by `deserialize_model_from_json`.

    Args:
        model (BaseModel): The model to serialize.

    Returns:
        str: A JSON string representation of the model.
    """
    dct = serialize_model(model)
    return json.dumps(dct)


def deserialize_model_from_json(data: str | bytes | bytearray) -> BaseModel:
    """Deserialize a model from a JSON string.

    Args:
        data (str | bytes | bytearray): The JSON string to deserialize.

    Returns:
        BaseModel: The deserialized Pydantic model.
    """
    dct = json.loads(data)
    model = _deserialize_model(dct)
    return model


def validate_model(value: BaseModel | dict | str, info: ValidationInfo) -> BaseModel:
    """Validate a pydantic model.

    This can be used in combination with `serialize_model` in the following way:

    ```python
    class Update[T: BaseModel](BaseModel):
        model: Annotated[
            T,
            PlainSerializer(serialize_model),
            PlainValidator(validate_model)
        ]
    ```

    Args:
        value (BaseModel | dict | str): The model value to validate.
        info (ValidationInfo): Information about the validation state.

    Raises:
        ValueError: If the value cannot be validated.

    Returns:
        BaseModel: The validated model.
    """

    match info.mode:

        case 'python':

            if isinstance(value, BaseModel):
                return value
            elif isinstance(value, dict):
                return _deserialize_model(value)
            else:
                raise ValueError(
                    f"unhandled type for mode {info.mode}: {type(value)}"
                )

        case 'json':

            if isinstance(value, str):
                return deserialize_model_from_json(value)
            elif isinstance(value, dict):
                return _deserialize_model(value)
            else:
                raise ValueError(
                    f"unhandled type for mode {info.mode}: {type(value)}"
                )

        case _:

            raise ValueError(f"Invalid mode: {info.mode}")
