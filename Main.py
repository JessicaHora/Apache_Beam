import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io.textio import WriteToText

from apache_beam.options.pipeline_options import PipelineOptions

Pipeline_Options = PipelineOptions(argv=None)
pipeline = beam.Pipeline(options=Pipeline_Options)

colunas_dengue = [
    'id',
    'data_iniSE',
    'casos',
    'ibge_code',
    'cidade',
    'uf',
    'cep',
    'latitude',
    'longitude']


def lista_para_dicionario(elemento, colunas):
    """
    recebe 2 listas
    Retorna um dicionario
    """
    return dict(zip(colunas, elemento))


def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)


def trata_datas(elemento):
    """
    Recebe um dicionario e cria um novo campo com ANO_MES
    Retorna o mesmo dicionario com o novo campo
    """
    # hash
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento


def chave_uf(elemento):
    """
    Receber um dicionario
    retornar uma Tupla com o estado(UF) e o elemento(UF,dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)


def casos_dengue(elemento):
    """
    Recebe uma tupla('RS', [{}, {}])
    Retornar uma tupla ('Rs-2014',8.0)
    """
    uf, registros = elemento
    for registro in registros:
        if bool(re.search(r'\d', registro['casos'])):
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
    else:
        yield (f"{uf}-{registro['ano_mes']}", 0.0)


def chave_uf_ano_mes_de_list(elemento):
    """
    Receber uma lista de elementos
    Retorna uma tupla contendo uma chave e o valor de chuva em mm
    ('UF-ANO-Mes'1.3)
    ['2016-01-24', '4.2','TO']
    """
    data, mm, uf = elemento
    ano_mes = '-'.join(data.split('-')[:2])
    chave = f'{uf}-{ano_mes}'
    if float(mm) < 0:
        mm = 0.0
    else:
        mm = float(mm)
    return chave, mm


def arredonda(elemento):
    """
    Recebe uma tupla
    Retorna uma tupla com o valor arredondado
    """
    chave, mm = elemento
    return (chave, round(mm, 1))


def filtra_campos_vazios(elemento):
    """
    Remove elementos que tenham chaves vazias
    Receber uma tupla ('CE-2015-01', {'chuvas': [85.8], 'dengue': [175.0]})
    Retorna uma tupla
    """
    chave, dados = elemento
    if all([
        dados['chuvas'],
        dados['dengue']
    ]):
        return True
    return False


def descompactar_elementos(elem):
    """
    Receber uma tupla ('CE-2015-11', {'chuvas': [0.4], 'dengue': [21.0]})
    Retornar uma tupla ('CE','2015','11','11','21.0')
    """
    chave, dados = elem
    chuva = dados['chuvas'][0]
    dengue = dados['dengue'][0]
    uf, ano, mes = chave.split('-')
    return uf, ano, mes, str(chuva), str(dengue)


def preparar_csv(elem, delimitador=';'):
    """
    Receber uma Tupla
    Retornar uma string delimitada
    """
    return f"{delimitador}".join(elem)


# apcolletions realiza a leitura do dataset usando o sdk do apache beam no caso readfromtext

dengue = (
        pipeline
        | "Leitura do dataset de dengue" >>
        ReadFromText('casos_dengue.txt', skip_header_lines=1)
        | "De texto para lista" >> beam.Map(texto_para_lista)
        | "De lista para Dicionario" >> beam.Map(lista_para_dicionario, colunas_dengue)
        | "Criar campo ano_mes" >> beam.Map(trata_datas)
        | "criar chave pelo estado" >> beam.Map(chave_uf)
        | "Agrupar pelo estado" >> beam.GroupByKey()
        | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
        | "Soma dos casos pela chave" >> beam.CombinePerKey(sum)
    #    |"Mostrar resultados" >> beam.Map(print)
)

chuvas = (
        pipeline
        | "Leitura de dataset chuvas" >>
        ReadFromText('chuvas.csv', skip_header_lines=1)
        | "De texto para lista(chuvas)" >> beam.Map(texto_para_lista, delimitador=',')
        | "Criando a chave UF_ANO_MES" >> beam.Map(chave_uf_ano_mes_de_list)
        | "Soma do total de chuvas pela chave" >> beam.CombinePerKey(sum)
        | "Arredondar resultados de chuvas" >> beam.Map(arredonda)
    # |"Mostrar resultados chuvas" >> beam.Map(print)
)
resultado = (
    # (chuvas,dengue)
    # | " Empilha as pcols " >> beam.Flatten()
    # | "Agrupa as pcpls " >> beam.GroupByKey()
        ({'chuvas': chuvas, 'dengue': dengue})
        | 'Mesclar pcols' >> beam.CoGroupByKey()
        | 'filtrar dados  vazios' >> beam.Filter(filtra_campos_vazios)
        | 'Descompactar elementos ' >> beam.Map(descompactar_elementos)
        | 'Preparar CSV' >> beam.Map(preparar_csv)
    # | "Mostrar resultados da união " >> beam.Map(print)
)

header = 'uf;ano;mes;dengue;chuva'

resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix='.csv', header=header)

pipeline.run()
