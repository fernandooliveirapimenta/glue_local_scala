import boto3
import getopt
import sys
import json

from botocore.config import Config

def handler_exception():
	ex_type, ex_value, ex_traceback = sys.exc_info()
	print(ex_value)
	sys.exit()

def job_run(args):

	job_name = args['job-name']
	job_parameters = eval(args['arguments'])
	region = args['region']
	profile = args['profile']

	boto3.setup_default_session(profile_name=profile)
    
	region_config = Config(
	    region_name = region
	)
    
	glue = boto3.client('glue', config=region_config)
    
	glue_response = glue.get_job(
	    JobName=job_name
	)
    
	glue_request = glue_response['Job']
    
	glue_request['JobName'] = glue_request['Name']
	del glue_request['Name']
	del glue_request['Role']
	del glue_request['CreatedOn']
	del glue_request['LastModifiedOn']
	del glue_request['ExecutionProperty']
	del glue_request['Command']
	glue_request['Arguments'] = glue_request['DefaultArguments']
	del glue_request['DefaultArguments']
	del glue_request['Connections']
	del glue_request['MaxRetries']
	del glue_request['GlueVersion']
	del glue_request['AllocatedCapacity']
	if 'NumberOfWorkers' in glue_request:
	    del glue_request['MaxCapacity']

	for key in job_parameters:
	    glue_request['Arguments'][key] = job_parameters[key]
        
	response = glue.start_job_run(
	    **glue_request
	)
    
	print(json.dumps(response, indent=4))
    
def job_clone(args):

	job_source = args['job-name-source']
	job_target = args['job-name-target']
	region_source = args['region-source']
	region_target = args['region-target']
	profile_source = args['profile-source']
	profile_target = args['profile-target']

	boto3.setup_default_session(profile_name=profile_source)

	region_source_config = Config(
	    region_name = region_source
	)

	glue_source = boto3.client('glue', config=region_source_config)

	glue_response = glue_source.get_job(
	    JobName=job_source
	)

	old_job_parameters = glue_response['Job']
	del old_job_parameters['Name']
	del old_job_parameters['CreatedOn']
	del old_job_parameters['LastModifiedOn']
	del old_job_parameters['AllocatedCapacity']
	if 'NumberOfWorkers' in old_job_parameters:
	    del old_job_parameters['MaxCapacity']

	sts_source = boto3.client('sts', config=region_source_config)

	account_id_source = sts_source.get_caller_identity()['Account']

	tags = glue_source.get_tags(
	    ResourceArn="arn:aws:glue:{}:{}:job/{}".format(region_source, account_id_source, job_source)
	)

	old_job_parameters['Name'] = job_target
	old_job_parameters.update({"Tags":tags['Tags']})
	old_job_parameters

	boto3.setup_default_session(profile_name=profile_target)

	region_target_config = Config(
	    region_name = region_target
	)

	sts_target = boto3.client('sts', config=region_target_config)

	account_id_target = sts_target.get_caller_identity()['Account']

	glue_target = boto3.client('glue', config=region_target_config)

	old_job_parameters['Role'] = old_job_parameters['Role'].replace(account_id_source, account_id_target)

	print(old_job_parameters)

	response = glue_target.create_job(
	    **old_job_parameters
	)

	print(json.dumps(response, indent=4))

def jobs_compare(args):

	job_source = args['job-name-source']
	job_target = args['job-name-target']
	region = args['region']
	profile = args['profile']

	boto3.setup_default_session(profile_name=profile)

	region_config = Config(
	    region_name = region
	)

	glue = boto3.client('glue', config=region_config)

	response_source = glue.get_job(
	    JobName=job_source
	)
	response_source = response_source['Job']
	del response_source['Name']
	del response_source['CreatedOn']
	del response_source['LastModifiedOn']

	response_target = glue.get_job(
	    JobName=job_target
	)
	response_target = response_target['Job']
	del response_target['Name']
	del response_target['CreatedOn']
	del response_target['LastModifiedOn']

	diff = {}
	for key in response_source:
		value = response_source[key]
		if isinstance(value, dict):
			diff_dict = {}
			for key_dict in value:
				value_dict = value[key_dict]
				if key_dict in response_target[key]:
					if str(value_dict) != str(response_target[key][key_dict]):
						diff_dict[key_dict] = "{} <> {}".format(str(value_dict), str(response_target[key][key_dict]))
				else:
					diff_dict[key_dict] = "{} <> {}".format(str(value_dict), "")
			if len(diff_dict) > 0:
				diff[key] = diff_dict
		else:
			if key in response_target:
				if str(value) != str(response_target[key]):
					diff[key] = "{} <> {}".format(str(value), str(response_target[key]))
			else:
				diff[key] = "{} <> {}".format(str(value), "")
		    
	for key in response_target:
		value = response_target[key]
		if isinstance(value, dict):
			diff_dict = diff[key] if key in diff else {}
			for key_dict in value:
				value_dict = value[key_dict]
				if not key_dict in response_source[key]:
					diff_dict[key_dict] = "{} <> {}".format("", str(value_dict))
			if len(diff_dict) > 0:
				diff[key] = diff_dict
		else:
			if not key in response_source:
				diff[key] = "{} <> {}".format("", str(value))

	if len(diff) > 0:
		print(json.dumps(diff, indent=4))
	else:
		print("Não existem diferenças")

def job_update_parameters(args):

	job_name = args['job-name']
	job_parameters = eval(args['parameters'])
	region = args['region']
	profile = args['profile']

	boto3.setup_default_session(profile_name=profile)

	region_config = Config(
		region_name = region
	)

	glue = boto3.client('glue', config=region_config)

	try:
		glue_response = glue.get_job(
			JobName=job_name
		)
	except:
		handler_exception()

	job_update = glue_response['Job']
	del job_update['Name']
	del job_update['CreatedOn']
	del job_update['LastModifiedOn']
	del job_update['AllocatedCapacity']
	if 'NumberOfWorkers' in job_update:
	    del job_update['MaxCapacity']

	for key in job_parameters:
		if isinstance(job_parameters[key], dict):
			job_update[key].update(job_parameters[key])
		else:
			job_update[key] = job_parameters[key]

	try:
		update_response = glue.update_job(
			JobName=job_name,
			JobUpdate=job_update)
	except:
		handler_exception()

	print(json.dumps(update_response, indent=4))

functions = {
	'job-run': {
		'args': ["job-name", "arguments", "region", "profile"],
		'func': job_run,
		'examples': [
			'--job-name bb30_carga_proposta_redis_residencial_historico --arguments "{\'--redis_ssl\': \'true\', \'--redis_auth\': \'/brseg/elasticache/brasilseg-redis/token\'}" --region us-east-1 --profile prd'
		]
	},
	'job-update-parameters': {
		'args': ["job-name", "parameters", "region", "profile"],
		'func': job_update_parameters,
		'examples': [
			'--job-name bb30_carga_proposta_redis_residencial_historico --parameters "{\'MaxRetries\':20}" --region us-east-1 --profile prd',
			'--job-name bb30_carga_proposta_redis_residencial_historico --parameters "{\'DefaultArguments\':{\'--redis_ssl\': \'true\', \'--redis_auth\': \'/brseg/elasticache/brasilseg-redis/token\'}}" --region us-east-1 --profile prd',
			'--job-name bb30_carga_proposta_redis_residencial_historico --parameters "{\'Command\':{\'ScriptLocation\': \'s3://datalake-glue-scripts-dev/carga_historica/carga_redis/novo_script.scala\'}}" --region us-east-1 --profile prd',
			'--job-name bb30_carga_proposta_redis_residencial_historico --parameters "{\'MaxRetries\':20,\'Command\':{\'ScriptLocation\': \'s3://datalake-glue-scripts-dev/carga_historica/carga_redis/novo_script.scala\'},\'DefaultArguments\':{\'--redis_ssl\': \'true\', \'--redis_auth\': \'/brseg/elasticache/brasilseg-redis/token\'}}" --region us-east-1 --profile prd'
		]
	},
	'job-clone': {
		'args': ["job-name-source", "job-name-target", "region-source", "region-target", "profile-source", "profile-target"],
		'func': job_clone,
		'examples': [
			'--job-name-source bb30_carga_proposta_redis_residencial_historico --region-source us-east-1 --profile-source prd --job-name-target bb30_carga_proposta_redis_residencial_incremental --region-target us-east-1 --profile-target prd'
		]
	},
	'jobs-compare': {
		'args': ["job-name-source", "job-name-target", "region", "profile"],
		'func': jobs_compare,
		'examples': [
			'--job-name-source bb30_carga_proposta_redis_residencial_historico --job-name-target bb30_carga_proposta_redis_residencial_incremental --region us-east-1 --profile prd'
		]
	}
}

call = "python3.7 {}".format(sys.argv[0])
args = sys.argv[1:]

function = ""
if len(args) > 0:
	function = args[0]
	args = args[1:]

if not function in functions:
	print("usage: {} <function> [options]".format(call))
	print("function:")
	for func in functions:
		print("   {}".format(func))
	sys.exit()

obj_function = functions[function]

opts = {}
last_arg = None
for arg in args:
	if last_arg:
		opts[last_arg.replace("--", "")] = arg
		last_arg = None
	else:
		last_arg = arg

requireds = obj_function['args']
examples = obj_function['examples']

for required in requireds:
	if not required in opts:
		print("usage: {} {}{}".format(call, function, " [options]" if len(requireds) > 0 else ""))
		if len(requireds) > 0:
			print("options:")
			for arg in requireds:
				print("  --{}".format(arg))
		print("examples:")
		for example in examples:
			print("  {} {} {}".format(call, function, example))
		sys.exit()

obj_function['func'](opts)
