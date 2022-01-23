#
#  SPDX-FileCopyrightText: 2021 Thorsten Hapke <thorsten.hapke@sap.com>
#
#  SPDX-License-Identifier: Apache-2.0
#
import argparse
import logging
import json


def main() :

    logging.basicConfig(level=logging.INFO)

    #
    # command line args
    #
    description =  "Prettify JSON-file. Load and saves it pretty."
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('-i','--indent', help = 'Indent spaces', type=int, default=4)
    parser.add_argument('jsonfile', help='Type of artifacts.')
    parser.add_argument('-o','--output', help='Output file. If not given input-file is changed.')

    args = parser.parse_args()

    output = args.output if args.output else args.jsonfile

    with open(args.jsonfile,'r') as fp:
        filecontent = json.load(fp)

    with open(output,'w') as fp :
        json.dump(filecontent,fp,indent=args.indent)

if __name__ == '__main__':
    main()