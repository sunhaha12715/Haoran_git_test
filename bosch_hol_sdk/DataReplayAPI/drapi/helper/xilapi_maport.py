from ASAM.XIL.Implementation.TestbenchFactory.Testbench import TestbenchFactory
from ASAM.XIL.Interfaces.Testbench.MAPort.Enum import MAPortState

import xml.etree.cElementTree as ET
from pathlib import Path
import logging
import json

class XILAPIMAPort:
    """
    Helper class to simplify the usage of the dSPACE XIL API MAPort. Creates a config file 
    automatically.
    """
    default_filename = "MAPortConfigDRAPI.xml"

    def __init__(self, platform, sdf_file, directory=".", logger=None) -> None:
        """
        Creates a XILAPIMAPort object

        platform - the dSPACE platform on which the application is loaded
        sdf_file - path to the sdf which is currently loaded to the dSPACE platform
        directory (optional) - the path where the MAPort Config is written. Default is '.'.
        logger (optional) - a logger instance. If none provided, one will be created automatically.
        """
        self._sdf_file = sdf_file
        self._platform = platform
        self._temp_directory = directory
        self._maport = None
        if logger is None:
            self._logger = logging.getLogger("XILAPIMAPort")
        else:
            self._logger = logger
        self._maport_config_file = XILAPIMAPort.create_maport_config(self._temp_directory, self._sdf_file, self._platform)
        self._init_testbench()

    def __del__(self):
        self.cleanup()

    def cleanup(self):
        """
        Frees any open ressources, e.g. connection to the XIL API. Needs to be called before the application can
        be unloaded. Also deletes the temporary created config file
        """
        # Disconnect from XIL API
        if self._maport is not None:
            self._maport.Dispose()
            self._maport = None
        # Delete the temporary created config file
        if self._maport_config_file.exists():
            self._maport_config_file.unlink()

    def _init_testbench(self):
        """
        Initializes the testbench and starts the simulation if necessary.
        """
        self._testbench_factory = TestbenchFactory()
        self._testbench = self._testbench_factory.CreateVendorSpecificTestbench("dSPACE GmbH", "XIL API", "2021-A")
        self._maport_factory = self._testbench.MAPortFactory
        self._value_factory = self._testbench.ValueFactory

        self._maport = self._maport_factory.CreateMAPort("ReplayMAPort")
        self._logger.debug("Created MAPort")
        self._maport_config = self._maport.LoadConfiguration(self._maport_config_file)
        self._maport.Configure(self._maport_config, False)
        self._logger.debug("Configured MAPort")
        if self._maport.State != MAPortState.eSIMULATION_RUNNING:
            self._maport.StartSimulation()
            self._logger.debug("Started simulation")

    def get_variables(self):
        """
        Returns a list of all available variables.        
        """
        return self._maport.VariableNames

    def read_variable(self, variable):
        """
        Reads and returns the value of a specific value

        variable - the variable to be read, e.g. 'DS6001()://Model Root/Enable/Value'
        """
        return self._maport.Read(variable).Value

    def is_one_dimensional(self, lst):
        """
        if the given list is one dimension
        """
        for item in lst:
            if isinstance(item, list):
                return False
        return True     
    
    def write_variable(self, variable, value):
        """
        Writes the value to the specified variable. The type is automatically deducted
        from the type of value. Currently supported types are int, float, and bool, list, matrix.

        variable - the variable the value should be written to, e.g. 'DS6001()://Model Root/Enable/Value'
        value - the value. Must be either type int, float, or bool
        """
        if isinstance(value, int):
            maport_value = self._value_factory.CreateIntValue(value)
        elif isinstance(value, float):
            maport_value = self._value_factory.CreateFloatValue(value)
        elif isinstance(value, bool):
            maport_value = self._value_factory.CreateBooleanValue(value)
        elif isinstance(value, list):
            if self.is_one_dimensional(value):
                if isinstance(value[0], int):
                    maport_value = self._value_factory.CreateIntVectorValue(value) 
                elif isinstance(value[0], float):
                    maport_value = self._value_factory.CreateFloatVectorValue(value)        
                elif isinstance(value[0], bool):
                    maport_value = self._value_factory.CreateBooleanVectorValue(value)
            else:
                if isinstance(value[0][0], float):
                    maport_value = self._value_factory.CreateFloatMatrixValue(value)                   
        else:
            raise TypeError(f"{type(value)} is not supported for writing")
        
        self._maport.Write(variable, maport_value)

    @classmethod
    def create_maport_config(cls, directory, sdf_path, platform):
        """
        Creates a MAPort config file in the given directory.
        """
        PortConfigurations = ET.Element("PortConfigurations")
        MAPortConfig = ET.SubElement(PortConfigurations, "MAPortConfig")

        ET.SubElement(MAPortConfig, "SystemDescriptionFile").text = sdf_path
        ET.SubElement(MAPortConfig, "PlatformName").text = platform
        ET.SubElement(MAPortConfig, "IncompatibilityBehavior").text = "0"

        tree = ET.ElementTree(PortConfigurations)
        filename = Path(directory) / cls.default_filename
        tree.write(filename, xml_declaration=True,encoding='utf-8', method="xml" , default_namespace="")
        return filename
