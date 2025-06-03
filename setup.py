import sys, os, re, shutil
from setuptools import setup, find_packages, Command
from distutils.command.build import build as _build
from setuptools.command.install import install as _install
from distutils.spawn import spawn
from glob import glob

systems = {
    'T0WmaDataSvc': {
        'python': ['T0WmaDataSvc'],
        'data': [],
        'bin': []
    }
}

def get_relative_path():
    return os.path.dirname(os.path.abspath(os.path.join(os.getcwd(), sys.argv[0])))

def define_the_build(dist, system_name, patch_x=''):
    docroot = "doc/build/html"
    system = systems[system_name]
    datasrc = sum((glob(f"src/{x}") for x in system['data']), [])
    binsrc = sum((glob(f"bin/{x}") for x in system['bin']), [])

    py_version = sys.version.split()[0]
    pylibdir = f'{patch_x}lib/python{py_version[0:3]}'
    
    dist.packages = find_packages(where='src/python')
    dist.package_dir = {'': 'src/python'}
    dist.data_files = [(f'{patch_x}bin', binsrc)]
    
    for dir in set(x[4:].rsplit('/', 1)[0] for x in datasrc):
        files = [x for x in datasrc if x.startswith(f'src/{dir}/')]
        dist.data_files.append((f'{patch_x}data/{dir}', files))

    if os.path.exists(docroot):
        for dirpath, dirs, files in os.walk(docroot):
            dist.data_files.append(
                (f"{patch_x}doc{dirpath[len(docroot):]}", 
                 [f"{dirpath}/{fname}" for fname in files if fname != '.buildinfo'])
            )

class BuildCommand(Command):
    description = "Custom build for T0WmaDataSvc"
    user_options = _build.user_options + [
        ('system=', 's', 'build the specified system (default: T0WmaDataSvc)'),
        ('skip-docs', None, 'skip documentation')
    ]

    def initialize_options(self):
        self.system = 'T0WmaDataSvc'
        self.skip_docs = False

    def finalize_options(self):
        if self.system not in systems:
            print(f"System {self.system} unrecognised, please use '-s T0WmaDataSvc'")
            sys.exit(1)

        define_the_build(self.distribution, self.system, '')
        shutil.rmtree(f"{get_relative_path()}/build", True)
        shutil.rmtree("doc/build", True)

    def generate_docs(self):
        if not self.skip_docs:
            os.environ["PYTHONPATH"] = f"{os.getcwd()}/build/lib:{os.environ.get('PYTHONPATH', '')}"
            spawn(['make', '-C', 'doc', 'html', 'PROJECT=.'])

    def run(self):
        self.run_command('build')
        self.generate_docs()

class InstallCommand(_install):
    description = "Custom install for T0WmaDataSvc"
    user_options = _install.user_options + [
        ('system=', 's', 'install the specified system (default: T0WmaDataSvc)'),
        ('patch', None, 'patch an existing installation (default: no patch)'),
        ('skip-docs', None, 'skip documentation')
    ]

    def initialize_options(self):
        super().initialize_options()
        self.system = 'T0WmaDataSvc'
        self.patch = None
        self.skip_docs = False

    def finalize_options(self):
        if self.system not in systems:
            print(f"System {self.system} unrecognised, please use '-s T0WmaDataSvc'")
            sys.exit(1)
        if self.patch and not os.path.isdir(f"{self.prefix}/xbin"):
            print(f"Patch destination {self.prefix} does not look like a valid location.")
            sys.exit(1)

        patch_flag = 'x' if self.patch else ''
        define_the_build(self.distribution, self.system, patch_flag)

        self.distribution.metadata.name = self.system
        super().finalize_options()

        if self.patch:
            self.install_lib = re.sub(r'(.*)/lib/python(.*)', r'\1/xlib/python\2', self.install_lib)
            self.install_scripts = re.sub(r'(.*)/bin$', r'\1/xbin', self.install_scripts)

    def run(self):
        super().run()

setup(
    name='t0wmadatasvc',
    version='2.1.0',
    maintainer_email='hn-cms-webInterfaces@cern.ch',
    packages=find_packages(where='src/python'),
    package_dir={'': 'src/python'},
    include_package_data=True,
    cmdclass={
        'build_system': BuildCommand,
        'install_system': InstallCommand
    }
)

