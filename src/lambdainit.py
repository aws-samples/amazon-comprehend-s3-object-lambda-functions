"""
Special initializations for Lambda functions.

This file must be imported as the first import in any file containing a Lambda function handler method.
"""

import sys

# add packaged dependencies to search path
sys.path.append('lib')
