""" unittest export.py """

# -*- coding: utf-8 -*-
import os
import pathlib
import shutil
import pytest
import json
import uuid

from machines import machine, factory, Serializer, Target, Input, Output, FileHandler


def test_import_export(tmpdir):
    """test import export machines"""
    workdir = tmpdir.mkdir("work")
    exportdir = tmpdir.mkdir("export")

    def save_data(dir, data):
        with open(pathlib.Path(dir) / "data.txt", "w") as fp:
            fp.write(data)

    def load_data(dir):
        with open(pathlib.Path(dir) / "data.txt") as fp:
            return fp.read()

    def copy_dir(dest, importdir):
        """copy data in importdir"""
        shutil.rmtree(dest, ignore_errors=True)
        shutil.copytree(importdir, dest)

    export_handler = FileHandler(load=lambda dirname: dirname)
    import_handler = FileHandler(save=copy_dir)

    # make machines
    @machine(output="A")
    def machineA():
        return "foobar"

    @machine(inputs={"data": Input("A", handler=export_handler)})  # no output
    def exporter(data, identifier_data, exportdir, records):
        """Export some data"""

        key = "some_random_key"
        outdir = pathlib.Path(exportdir) / key
        # here check A can be exported
        # ...
        records[identifier_data.index] = key
        shutil.copytree(data, outdir)

    @machine(
        inputs={"data": Input("A", handler=export_handler)},
        output=Output("B", handler=import_handler),
    )
    def importer(data, identifier_data, importdir, records):
        """Import some data"""

        key = records.get(identifier_data.index)
        if not key:
            raise Exception("Target not in records.")

        import_path = pathlib.Path(importdir) / key
        if not import_path.is_dir():
            raise Exception("Could not find directory")
        # compare new and old files
        if set(os.listdir(data)) != set(os.listdir(import_path)):
            raise Exception("Mismatch in existing and imported files")
        # return path for handler
        return import_path

    # export records
    records = {}
    # default handler
    handlers = {"default": {"save": save_data, "load": load_data}}
    with factory(root=workdir, handlers=handlers, hold=True):
        machineA.single("id1")
        export_task = exporter.single("id1", exportdir=exportdir, records=records)

    # check export dir
    assert "id1" in records
    assert (exportdir / records["id1"]).isdir()
    assert load_data(exportdir / records["id1"]) == "foobar"

    # modify data
    save_data(exportdir / records["id1"], "foobaz")

    # import data
    with factory(root=workdir, handlers=handlers, hold=True) as fy:
        import_task = importer.single("id1", importdir=exportdir, records=records)

    # check imported data
    assert fy.exists(Target("B", "id1"))
    assert fy.read(Target("B", "id1")) == "foobaz"
