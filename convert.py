with open("results.txt") as f:
    peers = {}
    headerOrder = []
    for l in f.readlines():
        if not l.strip():
            continue
        if l.startswith("#"):
            continue
        key, value = l.split("\t")
        key = key.strip()
        value = value.strip()
        if key == "numberOfPeers":
            currentNumberOfPeers = value
            if not peers.has_key(currentNumberOfPeers):
                peers[currentNumberOfPeers] = {}
        if key != "numberOfPeers":
            if key not in headerOrder:
                headerOrder.append(key)
            if not peers[currentNumberOfPeers].has_key(key):
                peers[currentNumberOfPeers][key] = []
            peers[currentNumberOfPeers][key].append(float(value))
    with open("results_converted.txt", "w") as f2:
        f2.write("\t")
        for npeers in sorted(peers.keys()):
            f2.write(npeers + "\t")
        f2.write("\n")

        nsamples = {}
        for header in headerOrder:
            f2.write(header + "\t")
            for npeers in sorted(peers.keys()):
                values = peers[npeers][header]
                if not nsamples.has_key(npeers):
                    nsamples[npeers] = len(values)
                else:
                    if len(values) < nsamples[npeers]:
                        nsamples[npeers] = len(values)

                f2.write(str(sum(values)/len(values)) + "\t")

            f2.write("\n" + header + "Max\t")
            for npeers in sorted(peers.keys()):
                values = peers[npeers][header]
                f2.write(str(max(values)) + "\t")
            f2.write("\n")

        for npeers in sorted(peers.keys()):
            print "%s %s samples" % (npeers, nsamples[npeers])
