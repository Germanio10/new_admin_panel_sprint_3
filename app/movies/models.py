import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.CharField(_('full_name'), max_length=50)

    def __str__(self):
        return self.full_name

    class Meta:
        db_table = '"content"."person"'
        verbose_name = _('Участник')
        verbose_name_plural = _('Участники')


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(_('name'), max_length=255)
    description = models.TextField(_('description'), blank=True, null=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = '"content"."genre"'
        verbose_name = _('Жанр')
        verbose_name_plural = _('Жанры')


class Filmwork(UUIDMixin, TimeStampedMixin):
    class TypeOfMovie(models.TextChoices):
        MOVIE = 'movie',
        TV_SHOW = 'tv_show',

    title = models.TextField(_('title'))
    description = models.TextField(_('description'), null=True)
    creation_date = models.DateField(null=True)
    file_path = models.FileField(
        _('file'),
        blank=True,
        null=True,
        upload_to='movies/')
    rating = models.FloatField(_('rating'), blank=True, null=True,
                               validators=[MinValueValidator(0),
                                           MaxValueValidator(100)])
    type = models.CharField(max_length=10, choices=TypeOfMovie.choices,
                            default=TypeOfMovie.MOVIE)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')
    persons = models.ManyToManyField(Person, through='PersonFilmwork')

    def __str__(self):
        return self.title

    class Meta:
        db_table = '"content"."film_work"'
        verbose_name = _('Фильм')
        verbose_name_plural = _('Фильмы')
        indexes = [
            models.Index(fields=['creation_date', 'rating'])
        ]


class GenreFilmwork(UUIDMixin):
    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = '"content"."genre_film_work"'
        unique_together = ['film_work', 'genre']


class PersonFilmwork(UUIDMixin):
    class TypeOfRole(models.TextChoices):
        ACTOR = 'actor',
        WRITER = 'writer',
        DIRECTOR = 'director'

    film_work = models.ForeignKey(Filmwork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(_('role'), choices=TypeOfRole.choices)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = '"content"."person_film_work"'
        unique_together = ['film_work', 'person', 'role']
