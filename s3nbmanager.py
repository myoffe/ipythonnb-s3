import datetime

from tornado import web

import boto

from IPython.html.services.notebooks.nbmanager import NotebookManager
from IPython.nbformat import current
from IPython.utils.traitlets import Unicode


class S3NotebookManager(NotebookManager):
    aws_access_key_id = Unicode(config=True, help='AWS access key id.')
    aws_secret_access_key = Unicode(config=True, help='AWS secret access key.')
    s3_bucket = Unicode('', config=True, help='Bucket name for notebooks.')
    s3_prefix = Unicode('', config=True, help='Key prefix of notebook location')
    notebook_dir = s3_prefix

    def __init__(self, **kwargs):
        super(S3NotebookManager, self).__init__(**kwargs)
        # Configuration of aws access keys default to '' since it's unicode.
        # boto will fail if empty strings are passed therefore convert to None
        access_key = self.aws_access_key_id if self.aws_access_key_id else None
        secret_key = self.aws_secret_access_key if self.aws_secret_access_key else None
        self.s3_con = boto.connect_s3(access_key, secret_key)
        self.bucket = self.s3_con.get_bucket(self.s3_bucket)

        self.mapping = {}
        self.load_notebook_names()

    def path_exists(self, path):
        return self.notebook_exists(path)

    def is_hidden(self, path):
        return False

    def notebook_exists(self, name, path=''):
        key = self.bucket.get_key(self.s3_prefix + name)
        return key is not None

    # TODO: Remove this after we create the contents web service and directories are
    # no longer listed by the notebook web service.
    def list_dirs(self, path):
        """List the directory models for a given API style path."""
        return []

    # TODO: Remove this after we create the contents web service and directories are
    # no longer listed by the notebook web service.
    def get_dir_model(self, name, path=''):
        """Get the directory model given a directory name and its API style path.

        The keys in the model should be:
        * name
        * path
        * last_modified
        * created
        * type='directory'
        """
        raise NotImplementedError('must be implemented in a subclass')

    def list_notebooks(self, path=''):
        models = []
        for key in self.bucket.list(self.s3_prefix):
            models.append(dict(name=key.name, path='', last_modifie=key.last_modified,
                               created=key.last_modified, type='notebook'))
        return models

    def get_notebook(self, name, path='', content=True):
        """Get the notebook model with or without content."""
        key = self.bucket.get_key(self.s3_prefix + name)
        last_modified = key.last_modified
        model = dict(name=name, path=name, last_modified=last_modified, created=last_modified, type='notebook')
        if content:
            model['content'] = key.get_contents_as_string()
        return model

    def save_notebook(self, model, name, path=''):
        """Save the notebook and return the model with no content."""
        nb = current.to_notebook_json(model['content'])
        # self.check_and_sign(nb, name, path)
        key = self.bucket.new_key(self.s3_prefix + name)
        key.set_contents_from_string(nb)
        return self.get_notebook(name, name, content=False)

    def update_notebook(self, model, name, path=''):
        """Update the notebook and return the model with no content."""
        new_name = model.get('name', name)
        if name != new_name:
            key = self.bucket.get_key(name)
            key.name = new_name
        return self.get_notebook(new_name)

    def delete_notebook(self, name, path=''):
        """Delete notebook by name and path."""
        self.bucket.delete_key(name)

    def create_checkpoint(self, name, path=''):
        """Create a checkpoint of the current state of a notebook

        Returns a checkpoint_id for the new checkpoint.
        """
        raise NotImplementedError("must be implemented in a subclass")

    def list_checkpoints(self, name, path=''):
        """Return a list of checkpoints for a given notebook"""
        return []

    def restore_checkpoint(self, checkpoint_id, name, path=''):
        """Restore a notebook from one of its checkpoints"""
        raise NotImplementedError("must be implemented in a subclass")

    def delete_checkpoint(self, checkpoint_id, name, path=''):
        """delete a checkpoint for a notebook"""
        raise NotImplementedError("must be implemented in a subclass")

    def info_string(self):
        return "Serving notebooks"

    def load_notebook_names(self):
        keys = self.bucket.list(self.s3_prefix)
        ids = [k.name.split('/')[-1] for k in keys]

        for id in ids:
            name = self.bucket.get_key(self.s3_prefix + id).get_metadata('nbname')
            self.mapping[id] = name

    # def list_notebooks(self):
    #     data = [dict(notebook_id=id, name=name) for id, name in self.mapping.items()]
    #     data = sorted(data, key=lambda item: item['name'])
    #     return data

    def read_notebook_object(self, notebook_id):
        if not self.notebook_exists(notebook_id):
            raise web.HTTPError(404, u'Notebook does not exist: %s' % notebook_id)
        try:
            key = self.bucket.get_key(self.s3_prefix + notebook_id)
            s = key.get_contents_as_string()
        except:
            raise web.HTTPError(500, u'Notebook cannot be read.')

        try:
            # v1 and v2 and json in the .ipynb files.
            nb = current.reads(s, u'json')
        except:
            raise web.HTTPError(500, u'Unreadable JSON notebook.')
        # Todo: The last modified should actually be saved in the notebook document.
        # We are just using the current datetime until that is implemented.
        last_modified = datetime.datetime.utcnow()
        return last_modified, nb

    def write_notebook_object(self, nb, notebook_id=None):
        try:
            new_name = nb.metadata.name
        except AttributeError:
            raise web.HTTPError(400, u'Missing notebook name')

        if notebook_id is None:
            notebook_id = self.new_notebook_id(new_name)

        try:
            data = current.writes(nb, u'json')
        except Exception as e:
            raise web.HTTPError(400, u'Unexpected error while saving notebook: %s' % e)

        try:
            key = self.bucket.new_key(self.s3_prefix + notebook_id)
            key.set_metadata('nbname', new_name)
            key.set_contents_from_string(data)
        except Exception as e:
            raise web.HTTPError(400, u'Unexpected error while saving notebook: %s' % e)

        self.mapping[notebook_id] = new_name
        return notebook_id

    def info_string(self):
        return "Serving notebooks from s3. bucket name: %s" % self.s3_bucket
