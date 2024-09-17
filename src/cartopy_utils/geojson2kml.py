def extract_kml(input_path, output_path, sector_filter, source_filter, limit=None):
    import simplekml
    import json

    with open(input_path) as f:
        data = json.load(f)
    kml = simplekml.Kml()
    count = 0
    for feature in data['features']:
        geom = feature['geometry']
        geom_type = geom['type']
        lake_id = str(feature["properties"]["LakeID"])
        source = feature["properties"]["Source"]
        sector = feature["properties"]["DrainageBa"]
        if source_filter and source != source_filter:
            continue
        if sector_filter and sector != sector_filter:
            continue
        if geom_type == 'MultiPolygon':
            kml.newpolygon(name=lake_id,
                           description=lake_id+" (source="+source+",sector="+sector+")",
                           outerboundaryis=geom['coordinates'][0][0])
            count += 1
            if limit and count >= limit:
                break

        elif geom_type == 'LineString':
            kml.newlinestring(name='test',
                              description='test',
                              coords=geom['coordinates'])
        elif geom_type == 'Point':
            kml.newpoint(name='test',
                         description='test',
                         coords=[geom['coordinates']])
        else:
            print("ERROR: unknown type:", geom_type)
    kml.save(output_path)

if __name__ == '__main__':

    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--inventory-path",help="path to geojson file containing inventory",default="/home/dev/Projects/greenland_lakes/lakes.geojson")
    parser.add_argument("--export-kml", help="export shapes to a KML file", default="lakes.kml")
    parser.add_argument("--sector", help="export only this sector", default="SW")
    parser.add_argument("--source", help="export only this source", default="S2")
    parser.add_argument("--limit", type=int, help="export only this many lakes", default=None)

    args = parser.parse_args()

    extract_kml(args.inventory_path,args.export_kml,args.sector,args.source,args.limit)