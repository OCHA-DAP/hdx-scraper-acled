Collector for ACLED’s Africa Datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

|Build Status| |Coverage Status|

Collector designed to collect the Africa datasets from the `ACLED`_
website.

Usage
~~~~~

::

    python run.py

You will need to have a file called .hdxkey in your home directory
containing only your HDX key for the register script to run. That script
was created to automatically register datasets on the `Humanitarian Data
Exchange`_ project.

.. _ACLED: http://www.acleddata.com/
.. _Humanitarian Data Exchange: http://data.humdata.org/

.. |Build Status| image:: https://travis-ci.org/OCHA-DAP/hdxscraper-acled-africa.svg?branch=master&ts=1
    :target: https://travis-ci.org/OCHA-DAP/hdxscraper-acled-africa
.. |Coverage Status| image:: https://coveralls.io/repos/github/OCHA-DAP/hdxscraper-acled-africa/badge.svg?branch=master&ts=1
    :target: https://coveralls.io/github/OCHA-DAP/hdxscraper-acled-africa?branch=master
