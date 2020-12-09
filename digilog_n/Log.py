##############################################################################
# log.py
# https://github.com/DigiLog-N/DigiLog-N
# Copyright 2020 Canvass Labs, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
# http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
##############################################################################
def log_file_main(user, password, subject, message, addressed_to):
    with open('/tmp/demo.log', 'a') as f:
        f.write("From (User): %s\n" % user)
        f.write("To: %s\n" % addressed_to)
        f.write("Password: %s\n" % password)
        f.write("Subject: %s\n" % subject)
        f.write("Message: %s\n" % message)
        f.write("########################################\n")
