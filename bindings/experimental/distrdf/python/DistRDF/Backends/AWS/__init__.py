## @author Vincenzo Eduardo Padulano
#  @author Enric Tejedor
#  @date 2021-02

################################################################################
# Copyright (C) 1995-2021, Rene Brun and Fons Rademakers.                      #
# All rights reserved.                                                         #
#                                                                              #
# For the licensing terms see $ROOTSYS/LICENSE.                                #
# For the list of contributors see $ROOTSYS/README/CREDITS.                    #
################################################################################

def RDataFrame(*args, **kwargs):
    """
    Create an RDataFrame object that can run computations on a Spark cluster.
    """

    from DistRDF.Backends.AWS import Backend
    aws = Backend.AWS(region_name=kwargs.pop("region_name", 'us-east-1'))
    return aws.make_dataframe(*args, **kwargs)
