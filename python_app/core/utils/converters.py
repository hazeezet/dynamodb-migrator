import numbers


def convert_to_dynamodb_type(value):
    """Convert Python values to DynamoDB format."""
    
    if isinstance(value, bool):
        
        return {'BOOL': value}
    
    elif isinstance(value, numbers.Number):
        
        return {'N': str(value)}
    
    elif isinstance(value, dict):
        
        return {'M': {k: convert_to_dynamodb_type(v) for k, v in value.items()}}
    
    elif isinstance(value, list):
        
        return {'L': [convert_to_dynamodb_type(elem) for elem in value]}
    
    elif value is None:
        
        return {'NULL': True}
    
    else:
        
        return {'S': value}
