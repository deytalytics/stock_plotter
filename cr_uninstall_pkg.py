import os

with open('requirements.txt', 'r', encoding='utf-16') as f:
    required_packages = f.read().splitlines()

with open('installed.txt', 'r', encoding='utf-16') as f:
    installed_packages = f.read().splitlines()

to_uninstall = [pkg for pkg in installed_packages if pkg not in required_packages]

with open('to_uninstall.txt', 'w', encoding='utf-16') as f:
    for pkg in to_uninstall:
        f.write(pkg+os.linesep)
