import os
import subprocess
import argparse
from json import loads
import logging

class AWSSinnk:
    """
    Class to streamline the creation of deployment packages towards AWS
    """
    def __init__(self, lambda_file_name, handler_function, dependencies):

        # Logging
        self.loglevel = logging.DEBUG
        logging.basicConfig()
        self.logger = logging.getLogger()
        self.logger.setLevel(self.loglevel)

        self.logger.info("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~")
        self.logger.info("         AWS Deployer v0.1")

        self.lambda_file_name = self.__set_lambda_file_name(lambda_file_name)
        self.archive_name = self.__set_archive_name(lambda_file_name)
        self.base_dir = os.getcwd()
        self.module_name = self.__set_module_name(lambda_file_name)
        self.logger.info(" Deploying Lambda function {}".format(self.module_name))

        self.handler_function = handler_function
        self.logger.debug("   Handler function {}".format(self.handler_function))
        self.dependencies = self.set_dependencies(dependencies)
        self.logger.debug("   Dependencies are {}".format(' '.join(self.dependencies)))

        self.region = "eu-west-1"
        self.logger.debug("   Region of deployment {}".format(self.region))
        self.role = ""
        self.profile = "adminuser"

    def set_role(self, role):
        self.role = role

    def set_region(self, region):
        self.region = region

    def set_profile(self, profile):
        self.profile = profile

    def set_dependencies(self, list_files_dirs):
        for path in list_files_dirs:
            if not os.path.exists(path):
                raise Exception("Path {} does not exist".format(path))

        return list_files_dirs

    def __set_module_name(self, filename):
        return filename.split(".")[0]

    def __set_lambda_file_name(self, filename):
        if os.path.isfile(filename):
            return filename
        else:
            raise Exception("File {} not found".format(filename))

    def __set_archive_name(self, filename):
        return filename.split(".")[0] + ".zip"

    def __create_deployment_package(self):
        self.logger.debug("   Create zip file {}".format(self.archive_name))
        subprocess.call(["zip", "-q9", self.archive_name, self.lambda_file_name])
        [subprocess.call(["zip", "-qr9", self.archive_name, path]) for path in self.dependencies]

    def __delete_deploymnet_package(self):
        if os.path.exists(self.archive_name):
            os.remove(self.archive_name)

    def __get_standard_args(self):
        comm_args = ["--region", self.region]
        comm_args += ["--function-name", self.module_name]
        comm_args += ["--zip-file", "fileb://{}/{}".format(self.base_dir, self.archive_name)]
        return comm_args

    def __create_function_args(self):
        if not self.role:
            raise ValueError("To create a new function, you must set the role of the Lambda function")

        comm_args = ['aws', 'lambda', 'create-function']
        comm_args += self.__get_standard_args()
        comm_args += ["--role", self.role]
        comm_args += ['--handler', "{}.{}".format(self.module_name, self.handler_function)]
        comm_args += ["--runtime", "python2.7"]
        comm_args += ['--profile', self.profile]

        return comm_args

    def __get_list_functions(self):
        res = subprocess.check_output(["aws", "lambda", "list-functions", "--region", self.region])
        return loads(res)

    def __check_function_exists(self, list_functions):
        for func in list_functions['Functions']:
            if self.module_name == func['FunctionName'] and self.role == self.role:
                return True
            elif self.module_name != func['FunctionName'] and self.role == self.role:
                raise Exception("Function role found but not the module/function name")
            elif self.module_name == func['FunctionName'] and self.role != self.role:
                raise Exception("Function/module name found but role not found")
            else:
                return False

    def __delete_function_args(self):
        if self.__check_function_exists(self.__get_list_functions()):
            self.logger.debug("   Delete already existing function")
            comm_args = ["aws", "lambda", "delete-function"]
            comm_args += ["--function-name", self.module_name]
            comm_args += ['--region', self.region]
            return comm_args
        else:
            return None

    def __update_function_args(self):
        comm_args = ['aws', 'lambda', 'update-function-code']
        comm_args += self.__get_standard_args()
        return comm_args

    def deploy_package(self, new=False):
        self.__delete_deploymnet_package()
        self.__create_deployment_package()

        if new:
            # First delete function
            comm_args = self.__delete_function_args()
            if comm_args:
                self.logger.info("  Deleting previous version of Lambda function")
                self.logger.debug(" Running command {}".format(" ".join(comm_args)))
                out = subprocess.check_output(comm_args)
                print out

            # Then re-create it
            comm_args = self.__create_function_args()
            self.logger.info("   Create new lambda function")
            self.logger.debug(" Running command {}".format(" ".join(comm_args)))
            out = subprocess.check_output(comm_args)
            print out
        else:
            comm_args = self.__update_function_args()
            self.logger.info("   Update lambda function with new code")
            self.logger.debug(" Running command {}".format(" ".join(comm_args)))
            out = subprocess.check_output(comm_args)
            print out


# Allow for filename to be passed as an argument. In which case,
# we will only push that file to ES. Otherwise, both the logging
# and offers templates will be updated.
parser = argparse.ArgumentParser(description='ES configuration deployment argument parser')
parser.add_argument('lambda_name', nargs="?", help="This is the filename where the template can be found.")
args = parser.parse_args()


def deploy_logging():
    dependencies = ["elasticsearch", "urllib3", "requests_aws4auth",
                    "requests", "requests-2.13.0.dist-info",
                    "requests_aws4auth-0.9.dist-info", "urllib3-1.20.dist-info"]
    aws_sink_logging = AWSSinnk("ingest_logging.py", "ingest_logging", dependencies)
    aws_sink_logging.set_role("arn:aws:iam::590746499688:role/shippy-logging-dev")
    aws_sink_logging.set_region("eu-west-1")
    aws_sink_logging.set_profile("adminuser")
    aws_sink_logging.deploy_package(new=False)


def deploy_offers():
    dependencies = ["elasticsearch", "urllib3", "requests_aws4auth",
                    "requests", "requests-2.13.0.dist-info",
                    "requests_aws4auth-0.9.dist-info", "urllib3-1.20.dist-info"]
    aws_sink_offers = AWSSinnk("ingest_newOffers.py", "ingest_newOffers", dependencies)
    aws_sink_offers.set_role("arn:aws:iam::590746499688:role/shippy-offers-dev")
    aws_sink_offers.set_region("eu-west-1")
    aws_sink_offers.set_profile("adminuser")
    aws_sink_offers.deploy_package(new=False)


def deploy_search():
    dependencies = ["elasticsearch", "urllib3", "requests_aws4auth",
                    "requests", "requests-2.13.0.dist-info",
                    "requests_aws4auth-0.9.dist-info", "urllib3-1.20.dist-info"]
    aws_sink_search_offers = AWSSinnk("search_offer.py", "search_offer", dependencies)
    aws_sink_search_offers.set_role("arn:aws:iam::590746499688:role/shippy-search-offer-dev")
    aws_sink_search_offers.set_region("eu-west-1")
    aws_sink_search_offers.set_profile("adminuser")
    aws_sink_search_offers.deploy_package(new=False)


def deploy_single_lambda(lambda_name):
    if lambda_name == "logging":
        deploy_logging()
    elif lambda_name == "offer":
        deploy_offers()
    elif lambda_name == "search":
        deploy_search()
    else:
        raise ValueError("Unknown lambda name. Authorized values are logging, offer, search.")


def deploy_all_lambda():
    deploy_logging()
    deploy_offers()
    deploy_search()

if args.lambda_name:
    deploy_single_lambda(args.lambda_name)
else:
    deploy_all_lambda()
