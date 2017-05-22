import subprocess
import argparse


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument('--file-name', '-f')

    args = parser.parse_args()
    
    file_name = args.file_name
    

    s3_file = 's3://gfw-files/sam/carbon_budget/{}'.format(file_name)
    cmd = ['aws', 's3', 'cp', s3_file, '.']
    subprocess.check_call(cmd)
        
        
if __name__ == '__main__':
    main()
