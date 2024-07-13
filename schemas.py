from marshmallow import Schema, fields, validate, validates, ValidationError

class DeviceDataSchema(Schema):
    device_name = fields.String(required=True, validate=validate.Length(max=50))
    device_type = fields.String(required=True, validate=validate.Length(max=50))
    timestamp = fields.DateTime(required=True)
    latitude = fields.Float(required=True)
    longitude = fields.Float(required=True)
    status = fields.String(allow_none=True, validate=validate.Length(max=50))
    water_level = fields.Integer(allow_none=True)
    action = fields.String(allow_none=True, validate=validate.Length(max=50))
    current_action = fields.String(allow_none=True, validate=validate.Length(max=50))

    @validates('latitude')
    def validate_latitude(self, value):
        if not (-90 <= value <= 90):
            raise ValidationError('Latitude must be between -90 and 90')

    @validates('longitude')
    def validate_longitude(self, value):
        if not (-180 <= value <= 180):
            raise ValidationError('Longitude must be between -180 and 180')

class LoginSchema(Schema):
    username = fields.String(required=True)
    password = fields.String(required=True)
