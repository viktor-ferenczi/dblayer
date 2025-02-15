""" Helpers to export the database graph in GraphML format

Requires the NetworkX library: http://networkx.lanl.gov/

"""

import networkx

from dblayer.model import database, table as table_model, column as column_model


class GMLExporter:

    def __init__(self, model):
        assert isinstance(model, database.Database)
        self.model = model

    def export(self, filepath):

        model = self.model
        assert isinstance(model, database.Database)

        g = networkx.MultiDiGraph()

        for table in model._table_list:
            if 0:
                assert isinstance(table, table_model.Table)
            title = table._name.upper()
            column_label_list = [
                '%s:%s%s%s' % (
                    column.name,
                    column.__class__.__name__,
                    ' NULL' if column.null else '',
                    '->' if isinstance(column, column_model.ForeignKey) else '')
                for column in table._column_list]
            label = '%s\n\n%s' % (title, '\n'.join(column_label_list))
            g.add_node(table._name)

        for table in model._table_list:
            for fk_column in table._column_list:
                if isinstance(fk_column, column_model.ForeignKey):
                    g.add_edge(
                        table._name,
                        fk_column.referenced_table._name,
                        label=fk_column.name)

        networkx.write_gml(g, filepath)
