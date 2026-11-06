from typing import Optional

from dvc.exceptions import DvcException
from dvc.fs import download as fs_download
from dvc.ignore import DvcIgnoreFilter
from dvc.output import Output
from dvc_objects.fs.scheme import Schemes


class DependencyDoesNotExistError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' does not exist"
        super().__init__(msg)


class DependencyIsNotFileOrDirError(DvcException):
    def __init__(self, path):
        msg = f"dependency '{path}' is not a file or directory"
        super().__init__(msg)


class DependencyIsStageFileError(DvcException):
    def __init__(self, path):
        super().__init__(f"DVC file '{path}' cannot be a dependency.")


class Dependency(Output):
    IS_DEPENDENCY = True

    DoesNotExistError: type[DvcException] = DependencyDoesNotExistError
    IsNotFileOrDirError: type[DvcException] = DependencyIsNotFileOrDirError
    IsStageFileError: type[DvcException] = DependencyIsStageFileError

    @property
    def dvcignore(self) -> Optional[DvcIgnoreFilter]:
        """
        For dependencies we override the dvcignore to be part of
        SCM root as well. Outputs cannot be saved outside the DVC repo.
        However, you can have dependency for subdir DVC repos.

        Returns:
            Optional[DvcIgnoreFilter]: DVC repo root or SCM root dvcignore.
        """
        if self.fs.protocol != Schemes.LOCAL:
            return None

        assert self.repo
        if self.fs.isin_or_eq(self.fs_path, self.repo.root_dir):
            return self.repo.dvcignore
        if self.fs.isin_or_eq(self.fs_path, self.repo.scm.root_dir):
            return self.repo.scm_dvcignore
        return None

    def workspace_status(self) -> dict[str, str]:
        if self.fs.version_aware:
            old_fs_path = self.fs_path
            try:
                self.fs_path = self.fs.version_path(self.fs_path, None)
                if self.changed_meta():
                    return {str(self): "update available"}
            finally:
                self.fs_path = old_fs_path
        return super().workspace_status()

    def update(self, rev=None):
        if self.fs.version_aware:
            self.fs_path = self.fs.version_path(self.fs_path, rev)
            self.meta = self.get_meta()
            self.fs_path = self.fs.version_path(self.fs_path, self.meta.version_id)

    def download(self, to, jobs=None):
        return fs_download(self.fs, self.fs_path, to.fs_path, jobs=jobs)

    def save(self):
        super().save()
        if self.fs.version_aware:
            self.fs_path = self.fs.version_path(self.fs_path, self.meta.version_id)

    def dumpd(self, **kwargs):
        if self.fs.version_aware:
            kwargs["with_files"] = True
        return super().dumpd(**kwargs)
