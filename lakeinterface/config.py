import boto3


DEFAULT_REGION = 'us-east-1'

def get_aws_session(aws_profile=None, region=DEFAULT_REGION):
    if aws_profile:
        return boto3.session.Session(profile_name=aws_profile)
    else:
        return boto3.session.Session(region_name=region)
        

def lake_config(config_group, aws_profile=None):
    session = get_aws_session(aws_profile=aws_profile)
    ssm_client = session.client('ssm')
    
    def _get_parameter(parameter_name):
        return ssm_client.get_parameter(Name=parameter_name)['Parameter']['Value']
    
    response = ssm_client.describe_parameters(
        ParameterFilters=[
            {
                'Key': 'Path',
                'Values': [f'/{config_group}']
            }
        ]
    )

    return dict(
        (p['Name'].replace(f'/{config_group}/',''), _get_parameter(p['Name'])) 
        for p in response['Parameters']
    )
