# ESStatistikParser
XML parser for ESStatistikListeModtag.xml

# What?

This application will, somewhat efficiently, split and parse a 75 GiB XML file containing vehicle information about registered vehicles in Denmark.

On an 8 core machine, it parses the file in about 8 minutes and uses less than 50 MB of RAM to do so.

# How?

The process is somewhat simple:

1. Produce logical XML chunks of one `<ns:Statistik/>` block per chunk.
2. Send the block to a buffered channel
3. Consume chunks in parallel across a number of CPU cores (see `-w` option)
4. Discard the parsed data (for now)

# How to get data file

Current data can be downloaded via FTP here: ftp://5.44.137.84/ESStatistikListeModtag/ (not my server).
