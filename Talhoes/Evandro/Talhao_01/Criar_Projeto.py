import requests
import geopandas as gpd
from urllib.request import urlopen
from urllib.error import URLError
from shutil import copyfileobj
import rasterio
from rasterio.merge import merge
import os
import shutil
import time
from qgis.core import (
    QgsApplication,
    QgsProject,
    QgsVectorLayer,
    QgsRasterLayer, 
    QgsRasterDataProvider, 
    QgsMultiBandColorRenderer, 
    QgsRasterBandStats, 
    QgsContrastEnhancement, 
    QgsDateTimeRange, 
    QgsRasterLayerTemporalProperties,
    QgsCoordinateTransform,
    QgsLayerTreeLayer,
    QgsVectorFileWriter
)
from PyQt5.QtCore import Qt
import datetime
import calendar
import argparse
def main(script_path):
    current_directory = os.path.dirname(os.path.abspath(script_path))
    api_key = "PLAK509fbde0851f4a14aa61bfd001ddf5ab"

    
    URL = "https://api.planet.com/basemaps/v1/mosaics"
    GPKG_PREFIX = "AOI_"
    OUTPUT_DIRECTORY = 'Imagens_Planet'
    DOWNLOAD_DIRECTORY = 'temporal_quads'
    from rasterio.warp import calculate_default_transform, reproject, Resampling
    def reproject_raster(in_path, out_path, crs):
        with rasterio.open(in_path) as src:
            transform, width, height = calculate_default_transform(src.crs, crs, src.width, src.height, *src.bounds)
            kwargs = src.meta.copy()

            kwargs.update({
                'crs': crs,
                'transform': transform,
                'width': width,
                'height': height
            })

            with rasterio.open(out_path, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs=crs,
                        resampling=Resampling.nearest
                    )

        return out_path
    common_crs = 'EPSG:4326'

    geojson_path = None
    current_directory = os.getcwd()
    for file in os.listdir(current_directory):
        if file.startswith("AOI_") and file.endswith(".gpkg"):
            geojson_path = os.path.join(current_directory, file)
            print("Found grid")
            break

    if not geojson_path:
        print("No AOI_*.gpkg file found in the current directory.")
    def download_file(url, file_path, retries=5, backoff_factor=1):
        for attempt in range(retries):
            try:
                with urlopen(url) as in_stream, open(file_path, 'wb') as out_file:
                    copyfileobj(in_stream, out_file)
                print(f"Downloaded: {file_path}")
                return
            except URLError as e:
                print(f"Error downloading {file_path}: {e}")
                if attempt < retries - 1:
                    backoff_time = backoff_factor * (2 ** attempt)
                    print(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                else:
                    raise

    def get_quads_with_retry(session, quads_url, search_parameters, retries=5, backoff_factor=1):
        for attempt in range(retries):
            try:
                res = session.get(quads_url, params=search_parameters, stream=True)
                res.raise_for_status()
                return res.json()
            except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
                print(f"Error fetching quads: {e}")
                if attempt < retries - 1:
                    backoff_time = backoff_factor * (2 ** attempt)
                    print(f"Retrying in {backoff_time} seconds...")
                    time.sleep(backoff_time)
                else:
                    raise

    def get_mosaic_details(session, url, months_2022, months_2023):
        mosaic_details = {}
        for year, months in zip([2022, 2023], [months_2022, months_2023]):
            for month in months:
                mosaic_name = f"planet_medres_visual_{year}-{month:02d}_mosaic"
                parameters = {"name__is": mosaic_name}
                res = session.get(url, params=parameters)
                if res.status_code != 200:
                    print(f"Failed to get mosaic for {mosaic_name}: {res.status_code}")
                    continue

                mosaic = res.json()
                if 'mosaics' in mosaic and mosaic['mosaics']:
                    mosaic_id = mosaic['mosaics'][0]['id']
                    mosaic_details[mosaic_name] = mosaic_id
                else:
                    print(f"No mosaics found for {mosaic_name}")
        return mosaic_details
    URL = "https://api.planet.com/basemaps/v1/mosaics"
    session = requests.Session()
    session.auth = (api_key, "")

    months_2022 = range(6, 13)
    months_2023 = range(1, 5)

    footprint = gpd.read_file(geojson_path)
    footprint = footprint.to_crs(epsg=4326)

    minx, miny, maxx, maxy = footprint.total_bounds
    bbox_coordinates = [minx, miny, maxx, maxy]
    string_bbox_coordinates = ','.join(map(str, bbox_coordinates))

    mosaic_details = get_mosaic_details(session, URL, months_2022, months_2023)

    for mosaic_name, mosaic_id in mosaic_details.items():
        year, month = map(int, mosaic_name.split('_')[3].split('-'))
        
        search_parameters = {'bbox': string_bbox_coordinates, 'minimal': True}
        quads_url = f"{URL}/{mosaic_id}/quads"
        
        quads = get_quads_with_retry(session, quads_url, search_parameters)
        items = quads['items']
        
        download_dir = 'temporal_quads'
        os.makedirs(download_dir, exist_ok=True)
        
        tiff_files = []
        for i in items:
            link = i['_links']['download']
            name = i['id']
            tiff_path = os.path.join(download_dir, name + '.tiff')

            try:
                download_file(link, tiff_path)
                tiff_files.append(tiff_path)
            except URLError as e:
                print(f"Failed to download {tiff_path} after retries: {e}")
                continue
        
        output_tiff = f'{month:02d}_{year}.tiff'
        output_reprojected_tiff = output_tiff
        src_files_to_mosaic = []
        for fp in tiff_files:
            src = rasterio.open(fp)
            src_files_to_mosaic.append(src)
        
        mosaic, out_trans = merge(src_files_to_mosaic)
        
        out_meta = src.meta.copy()
        out_meta.update({
            "driver": "GTiff",
            "height": mosaic.shape[1],
            "width": mosaic.shape[2],
            "transform": out_trans
        })
        
        date_string = f"{year}-{month:02d}-01"
        date_tag = datetime.datetime.strptime(date_string, '%Y-%m-%d').strftime('%Y:%m:%d')
        
        out_meta.update({
            "TAGS": {
                "DATE": date_tag
            }
        })
        
        with rasterio.open(output_tiff, "w", **out_meta) as dest:
            dest.write(mosaic)
            dest.update_tags(DATE=date_tag)
        reproject_raster(output_tiff, output_reprojected_tiff, common_crs)
        print(f"Merged TIFF saved as: {output_tiff}")

        for src in src_files_to_mosaic:
            src.close()

        shutil.rmtree(download_dir)
        print(f"Deleted directory: {download_dir}")
        
        output_dir = 'Imagens_Planet'
        os.makedirs(output_dir, exist_ok=True)
        
        final_output_path = os.path.join(output_dir, output_tiff)
        shutil.move(output_reprojected_tiff, final_output_path)
        
        print(f"Moved {output_reprojected_tiff} to {final_output_path}")
    ### Precisa saber onde o QGIS do usuario esta instalado.
    #### path_qgis = "/usr"


    project = QgsProject.instance()

    #Criando grupo dos tiffs para serem adicionados

    group_name = "TIFF Layers"
    group = project.layerTreeRoot().insertGroup(0, group_name)
    # Get the current directory
    

    # Search for .gpkg files
    apontamentos_path = None
    AOI_path = None
    duvidas_path = None
    talhoes_original_path = None


    # Função para ajustar a renderização do raster RGB
    def adjust_rgb_contrast(layer: QgsRasterLayer, red_band: int = 1, green_band: int = 2, blue_band: int = 3) -> None:
        provider: QgsRasterDataProvider = layer.dataProvider()
        renderer = layer.renderer()

        if isinstance(renderer, QgsMultiBandColorRenderer):
            for band in [red_band, green_band, blue_band]:
                stats: QgsRasterBandStats = provider.bandStatistics(band, QgsRasterBandStats.All, layer.extent(), 0)
                min_val = max(stats.minimumValue, 0)
                max_val = max(stats.maximumValue, 0)

                enhancement = QgsContrastEnhancement(renderer.dataType(band))
                contrast_enhancement = QgsContrastEnhancement.StretchToMinimumMaximum
                enhancement.setContrastEnhancementAlgorithm(contrast_enhancement, True)
                enhancement.setMinimumValue(min_val)
                enhancement.setMaximumValue(max_val)
                renderer.setContrastEnhancementForBand(band, enhancement)

            layer.setRenderer(renderer)
            layer.triggerRepaint()

    def set_band_based_on_range(layer: QgsRasterLayer, t_range: QgsDateTimeRange) -> None:
        tprops: QgsRasterLayerTemporalProperties = layer.temporalProperties()
        if tprops.isVisibleInTemporalRange(t_range) and t_range.begin().isValid() and t_range.end().isValid():
            if tprops.mode() == QgsRasterLayerTemporalProperties.ModeFixedTemporalRange:
                layer_t_range: QgsDateTimeRange = tprops.fixedTemporalRange()
                start: datetime.datetime = layer_t_range.begin().toPyDateTime()
                end: datetime.datetime = layer_t_range.end().toPyDateTime()
                delta = (end - start) / layer.bandCount()
                band_num = int((t_range.begin().toPyDateTime() - start) / delta) + 1
                adjust_rgb_contrast(layer, band_num, band_num + 1, band_num + 2)

    def set_fixed_temporal_range(layer: QgsRasterLayer, t_range: QgsDateTimeRange) -> None:
        mode = QgsRasterLayerTemporalProperties.ModeFixedTemporalRange
        tprops: QgsRasterLayerTemporalProperties = layer.temporalProperties()
        tprops.setMode(mode)
        if t_range.begin().timeSpec() == 0 or t_range.end().timeSpec() == 0:
            begin = t_range.begin()
            end = t_range.end()
            begin.setTimeSpec(Qt.TimeSpec(1))
            end.setTimeSpec(Qt.TimeSpec(1))
            t_range = QgsDateTimeRange(begin, end)
        tprops.setFixedTemporalRange(t_range)
        tprops.setIsActive(True)

    def process_raster_files_in_directory(directory_path):
        for filename in os.listdir(directory_path):
            print("carregando ")
            if filename.endswith('.tiff'):
                try:
                    month, year = map(int, filename.split('.')[0].split('_'))
                    last_day = calendar.monthrange(year, month)[1]
                    
                    middle_day = (last_day // 2) + 1

                    start_date = f'{year}:{month}:1'
                    end_date = f'{year}:{month}:{middle_day}'
                    start_time = '0:0:0'
                    end_time = '0:0:0'

                    date_time = [start_date, start_time, end_date, end_time]
                    dt_dict = {}
                    key = 0
                    for i in date_time:
                        a = i.split(':')
                        for j in range(len(a)):
                            key += 1
                            dt_dict[key] = int(a[j])

                    raster_path = os.path.join(directory_path, filename)

                    layer = QgsRasterLayer(raster_path, filename)
                    if not layer.isValid():
                        print(f'Falha ao carregar o raster {filename}!')
                        continue
                    project.addMapLayer(layer, False)
                    group.addLayer(layer)

                    start = datetime.datetime(dt_dict[1], dt_dict[2], dt_dict[3])
                    end = datetime.datetime(dt_dict[7], dt_dict[8], dt_dict[9])
                    set_fixed_temporal_range(layer, QgsDateTimeRange(start, end))

                    print(f'Raster {filename} processado com data de {start} a {end}.')

                except Exception as e:
                    print(f'Erro ao processar o arquivo {filename}: {e}')
    folder_name = "Imagens_Planet"

    # Construct the path to the folder
    folder_path = os.path.join(current_directory, folder_name)
    process_raster_files_in_directory(folder_path)
    for file in os.listdir(current_directory):
        if file == "apontamentos.gpkg":
            apontamentos_path = os.path.join(current_directory, file)
        elif file.startswith("AOI_") and file.endswith(".gpkg"):
            AOI_path = os.path.join(current_directory, file)
        elif file == "duvidas.gpkg":
            duvidas_path = os.path.join(current_directory, file)
        elif file == "talhoes_original.gpkg":
            talhoes_original_path = os.path.join(current_directory, file)

    def add_layer_to_project(layer_path, layer_name, layer_position, crs=None):
        gpkg_layer = f"{layer_path}|layername={layer_name}"
        layer = QgsVectorLayer(gpkg_layer, layer_name, "ogr")
        if not layer.isValid():
            print(f"Layer {layer_name} failed to load!")
            return None
        else:
            if crs:
                layer.setCrs(crs)
            QgsProject.instance().addMapLayer(layer, False)
            layerTree = QgsProject.instance().layerTreeRoot()
            layerTree.insertChildNode(layer_position, QgsLayerTreeLayer(layer))
            print(f"Layer {layer_name} CRS: {layer.crs().authid()}")
            print(f"Layer {layer_name} Extent: {layer.extent().toString()}")
            return layer

    common_crs = None
    common_extent = None

    if apontamentos_path is not None:
        apontamentos_layer = add_layer_to_project(apontamentos_path, "apontamentos", 0)
        apontamentos_layer.loadNamedStyle(current_directory+"/Styles/apontamentos.qml")
        if apontamentos_layer:
            common_crs = apontamentos_layer.crs()
            common_extent = apontamentos_layer.extent()
            print(f"Common CRS set from apontamentos layer: {common_crs.authid()}")
            print(f"Common Extent set from apontamentos layer: {common_extent.toString()}")

    if duvidas_path is not None:
        duvidas_layer = add_layer_to_project(duvidas_path, "duvidas", 1, common_crs)
        duvidas_layer.loadNamedStyle(current_directory+"/Styles/duvidas.qml")

    if talhoes_original_path is not None:
        talhoes_original_layer = add_layer_to_project(talhoes_original_path, "talhoes_original", 2, common_crs)
        talhoes_original_layer.loadNamedStyle(current_directory+"/Styles/original.qml")
        if talhoes_original_layer is not None:
            talhoes_editados_layer = talhoes_original_layer.clone()
            talhoes_editados_layer.setName("talhoes_editados")
            talhoes_editados_layer.setCrs(common_crs)
            talhoes_editados_layer.loadNamedStyle(current_directory+"/Styles/editado.qml")
            QgsProject.instance().addMapLayer(talhoes_editados_layer, False)
            layerTree = QgsProject.instance().layerTreeRoot()
            layerTree.insertChildNode(3, QgsLayerTreeLayer(talhoes_editados_layer))
            print(f"talhoes_editados_layer CRS: {talhoes_editados_layer.crs().authid()}")
            print(f"talhoes_editados_layer Extent: {talhoes_editados_layer.extent().toString()}")
            output_path = current_directory + "/talhoes_editados.geojson"  # ou .gpkg para GeoPackage
            error = QgsVectorFileWriter.writeAsVectorFormat(
    talhoes_editados_layer,
    output_path,
    "utf-8",
    talhoes_editados_layer.crs(),
    "GeoJSON"  # Especifica o formato GeoJSON
)
            if error == QgsVectorFileWriter.NoError:
                print("Camada 'talhoes_editados' salva com sucesso em:", output_path)
            else:
                print("Erro ao salvar a camada:", error)
           
    if AOI_path is not None:
        AOI_layer = add_layer_to_project(AOI_path, "aoi", 4)
        if AOI_layer:
            print(f"AOI_layer CRS before reprojection: {AOI_layer.crs().authid()}")
            print(f"AOI_layer Extent before reprojection: {AOI_layer.extent().toString()}")

            if AOI_layer.crs() != common_crs:
                print(f"Reprojecting AOI_layer from {AOI_layer.crs().authid()} to {common_crs.authid()}")
                transform_context = QgsProject.instance().transformContext()
                coordinate_transform = QgsCoordinateTransform(AOI_layer.crs(), common_crs, transform_context)

                AOI_layer.startEditing()
                for feature in AOI_layer.getFeatures():
                    geom = feature.geometry()
                    geom.transform(coordinate_transform)
                    AOI_layer.changeGeometry(feature.id(), geom)
                AOI_layer.commitChanges()

                AOI_layer.setCrs(common_crs)
                AOI_layer.updateExtents()
                print(f"AOI_layer CRS after reprojection: {AOI_layer.crs().authid()}")
                print(f"AOI_layer Extent after reprojection: {AOI_layer.extent().toString()}")

            QgsProject.instance().addMapLayer(AOI_layer, False)
            layerTree = QgsProject.instance().layerTreeRoot()
            layerTree.insertChildNode(4, QgsLayerTreeLayer(AOI_layer))
            AOI_layer.loadNamedStyle(current_directory+"/Styles/grid.qml")

    project.write('r01.qgz')
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Processa mosaicos do Planet e manipula camadas no QGIS.")
    parser.add_argument("script_path", help="Caminho do script a ser utilizado como parâmetro")
    args = parser.parse_args()
    main(args.script_path)