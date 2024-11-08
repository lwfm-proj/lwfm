""" A script to import all of the metasheets from one MetaRepo, and export to another """
import sys
import requests

# TODO 

def identity_transform(metasheet):
    """ This is meant to be overridden by users
    For example, if you want to edit an s3 url for each metasheet, or change the target type
    By default, do nothing """
    return metasheet

def base_import(params):
    """
    Import functions take in a set of parameters. This is a base import that will pull from a
    preexisting metarepo. It needs the metarepo's URL and a token.

    Import functions are generators that yield metasheets, one at a time, until the source is depleted
    """
    import_url = params['import_url']
    import_token = params['import_token']
    if import_url[-1] == '/': # Strip possible trailing slashes
        import_url = import_url[:-1]

    finished = False
    page = 0

    while not finished:
        # This should return a list of up to 1000 metasheets
        res_im = requests.get(f"{import_url}/metarepo/admin/find_all",
                          headers={"Authorization" : f"Bearer {import_token}"},
                          data={"page" : page},
                          timeout=10)

        # We should get a list of metasheets
        # check the status code, process the sheets, then possibly continue
        if res_im.status_code != 200:
            sys.exit(f"Received status code {res_im.status_code} from import with message: "
                     f"{res_im.text}")

        metasheets = res_im.json()
        for metasheet in metasheets:
            yield metasheet

        if len(metasheets) < 1000 :
            finished = True

def base_export(metasheet, params):
    """ Export functions take in a metasheet and a set of parameters, then put them *somewhere*.
    This base_export will put it to a preexisting metarepo. It needs the metarepo's URL and a token."""

    export_url = params['export_url']
    export_token = params['export_token']
    if export_url[-1] == '/':
        export_url = export_url[:-1]

    res_ex = requests.post(f"{export_url}/metarepo/admin/forceNotate",
                      headers={"Authorization" : f"Bearer {export_token}"},
                      data={"metasheet" : metasheet},
                      timeout=10)

    if res_ex.status_code != 200:
        sys.exit(f"Received status code {res_ex.status_code} from export with message: "
                 f"{res_ex.text}")

def import_export(import_function=base_import, import_params={},
                  export_function=base_export, export_params={},
                  transform=identity_transform):
    """ Import metasheets from one location, perform an optional transformation, then export
    the transformed and filtered metasheets to another location"""

    for metasheet in import_function(import_params):
        metasheet =  transform(metasheet)
        if not metasheet:
            continue

        export_function(metasheet, export_params)



def main():
    """ By default, we take in arguments from command line and pass them in to import_export """
    if sys.argv != 5:
        sys.exit("Usage: import_export.py <import_url> <import_token> <export_url> <export_token>")

    import_url = sys.argv[1]
    import_token = sys.argv[2]
    export_url = sys.argv[3]
    export_token = sys.argv[4]
    import_params = {'import_url' : import_url,
                     'import_token' : import_token}
    export_params = {'export_url' : export_url,
                     'export_token' : export_token}
    import_export(base_import, import_params, base_export, export_params)

if __name__ == "__main__":
    main()
