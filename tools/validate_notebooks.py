"""Validate scenario notebook formats and Python syntax without starting Spark."""

import ast
import json
import re
from pathlib import Path

import nbformat


def main():
    root = Path(__file__).resolve().parents[1]
    notebooks = sorted((root / 'reference_solutions').glob('Scenerio*.ipynb'))
    notebooks += sorted((root / 'practice').glob('*.ipynb'))
    scripts = sorted((root / 'reference_solutions').glob('Scenerio*.py'))
    errors = []
    code_cells = 0
    for path in notebooks:
        try:
            notebook = json.loads(path.read_text(encoding='utf-8'))
            nbformat.validate(notebook)
            source_blocks = [
                ''.join(cell['source'])
                for cell in notebook['cells'] if cell['cell_type'] == 'code'
            ]
            for index, source in enumerate(source_blocks, 1):
                if source.lstrip().startswith('%run'):
                    if not re.fullmatch(r'%run ./_setup \$exercise_id="(?:dashboard|[1-9][0-9]*)"\s*', source):
                        raise ValueError('Unrecognized shared-setup magic')
                    if not (path.parent / '_setup.ipynb').is_file():
                        raise ValueError('Missing shared setup notebook')
                else:
                    compile(source, f'{path.name}:cell{index}', 'exec')
            code_cells += len(source_blocks)
        except (ValueError, SyntaxError, nbformat.ValidationError) as exc:
            errors.append(f'{path.name}: {exc}')

    for path in scripts:
        try:
            source = path.read_text(encoding='utf-8')
            if not source.startswith('# Databricks notebook source\n'):
                raise ValueError('Missing Databricks source-notebook header')
            tree = ast.parse(source, filename=path.name)
            if any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                   and node.func.id == 'SparkContext' for node in ast.walk(tree)):
                raise ValueError('Creates a SparkContext unsupported on serverless compute')
            compile(tree, path.name, 'exec')
        except (ValueError, SyntaxError) as exc:
            errors.append(f'{path.name}: {exc}')

    for path in (root / 'Datasets').glob('*.json'):
        try:
            json.loads(path.read_text(encoding='utf-8'))
        except ValueError as exc:
            errors.append(f'{path.name}: {exc}')

    if errors:
        raise SystemExit('\n'.join(errors))
    print(f'PASS: {len(notebooks)} Jupyter notebooks ({code_cells} code cells), '
          f'{len(scripts)} Databricks Python notebooks, and JSON fixtures.')
    print('Format and syntax validation only; this does not execute Spark transformations.')


if __name__ == '__main__':
    main()
