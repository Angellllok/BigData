import gmplot
import numpy as np
import os


def pair_iterator(iterable):
    it = iter(iterable)
    for x, y in zip(it, it):
        yield x, y

if __name__ == "__main__":
    dataset = []
    print os.getcwd()
    with open("/home/ira/lab3hadoop/output01/part-r-00000", "r") as file:
        for l in file:
            record = l.strip()[10:].split("#")
            coords = np.array([(int(b), int(a)) for a, b in pair_iterator(record[6:]) if int(a) and int(b)])
            dataset.append((coords, record[0], record[1]))

    gmap = gmplot.GoogleMapPlotter(47.5, -121.65, 16)
    for location in dataset:
        latlon = location[0] / 1000000
        gmap.plot(latlon[:, 0], latlon[:, 1], "red", edge_width=2)
        if location[1] or location[2]:
            gmap.marker(latlon[:, 0].mean(), latlon[:, 1].mean(), title="{}, {}".format(location[1], location[2]))
    gmap.draw("result.html")
