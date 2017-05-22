import subprocess
import argparse


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--file-name', '-f')
    parser.add_argument('--upload', dest='upload', action='store_true')
    parser.add_argument('--download', dest='upload', action='store_true')
    parser.set_defaults(upload=False)
    parser.set_defaults(download=False)

    args = parser.parse_args()
    
    file_name = args.file_name
        
    if upload:
        cmd = ['aws', 's3', 'cp', file_name, 's3://gfw-files/sam/carbon_budget/']
        subprocess.check_call(cmd)
    
    if download:
        s3_file = 's3://gfw-files/sam/carbon_budget/{}'.format(file_name)
        cmd = ['aws', 's3', 'cp', s3_file, '.']
        subprocess.check_call(cmd)
        
if __name__ == '__main__':
    main()
