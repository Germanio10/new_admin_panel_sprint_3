from django.contrib.postgres.aggregates import ArrayAgg
from django.db import models
from django.db.models import Q
from django.http import JsonResponse
from django.views import View
from django.views.generic.detail import BaseDetailView
from django.views.generic.list import BaseListView

from movies.models import Filmwork


class TypeOfRole(models.TextChoices):
    ACTOR = 'actor',
    DIRECTOR = 'director',
    WRITER = 'writer'


class MoviesApiMixin(View):
    model = Filmwork
    http_method_names = ['get']

    def get_queryset(self):
        queryset = Filmwork.objects.prefetch_related('genres', 'persons').values(
            'id', 'title', 'description', 'creation_date',
            'rating', 'type'
        ).annotate(
            genres=ArrayAgg('genres__name', distinct=True),
            actors=ArrayAgg(
                'persons__full_name',
                filter=Q(
                    personfilmwork__role=TypeOfRole.ACTOR),
                distinct=True),
            directors=ArrayAgg(
                'persons__full_name',
                filter=Q(
                    personfilmwork__role=TypeOfRole.DIRECTOR),
                distinct=True),
            writers=ArrayAgg(
                'persons__full_name',
                filter=Q(
                    personfilmwork__role=TypeOfRole.WRITER),
                distinct=True),
        ).all()
        return queryset

    def get_context_data(self, *, object_list=None, **kwargs):
        queryset = self.get_queryset()
        paginator, page, queryset, is_paginated = self.paginate_queryset(
            queryset, self.paginate_by
        )
        context = {
            'count': paginator.count,
            'total_pages': paginator.num_pages,
            'prev': page.previous_page_number() if page.has_previous() else None,
            'next': page.next_page_number() if page.has_next() else None,
            'results': list(self.get_queryset())
        }
        return context

    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context)


class MoviesListApi(MoviesApiMixin, BaseListView):

    paginate_by = 50


class MoviesDetailApi(MoviesApiMixin, BaseDetailView):

    def get_context_data(self, *, object_list=None, **kwargs):
        return {**kwargs['object']}
