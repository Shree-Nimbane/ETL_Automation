import pytest

import sys
sys.dont_write_bytecode = True


def main():
    pytest_args=["tests",
                 '-v',
                 "--tb=short",
                 "--capture=tee-sys"]

    sys.exit(pytest.main(pytest_args))

if __name__=="__main__":
    main()
