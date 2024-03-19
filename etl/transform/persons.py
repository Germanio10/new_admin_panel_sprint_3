from common.transform_abstract import TransformAbstract
from elasticsearch_models import PersonElasticsearchDataRow, \
    PersonElasticsearchDataTransform
from extract.persons import ExtractPersons


class PersonsTransform(TransformAbstract):
    actions_list = []

    def __init__(self, first_load: bool = False):
        self.persons_data, self.object_with_updated_at = ExtractPersons('person', first_load).start_process()

    def transform(self) -> list[list]:
        es_transform_data = {}
        for person in self.persons_data:
            person = PersonElasticsearchDataRow(*person)

            if person.id in es_transform_data:
                person_films = es_transform_data[person.id].films
                if person.fw_id in person_films:
                    if person.role not in person_films[person.fw_id]['roles']:
                        person_films[person.fw_id]['roles'].append(person.role)
                else:
                    person_films[person.fw_id] = {
                        'id': person.fw_id,
                        'roles': [person.role]
                    }
            else:
                es_transform_data[person.id] = PersonElasticsearchDataTransform(
                    id=person.id,
                    full_name=person.full_name,
                    films={
                        person.fw_id: {
                            'id': person.fw_id,
                            'roles': [person.role]
                        }
                    }
                )

        for person_id, person_data in es_transform_data.items():
            self.actions_list.append({
                '_id': person_id,
                '_index': 'persons',
                '_source': {
                    'id': person_data.id,
                    'full_name': person_data.full_name,
                    'films': [films for films in person_data.films.values()]
                },
            })
        return [self.actions_list, self.object_with_updated_at]
