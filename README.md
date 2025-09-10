# LehrerDB v2 - Schulverwaltung

Eine einfache Webanwendung zur Verwaltung von Schuldaten wie Schülern, Klassen, Noten und Anwesenheiten.

## Features

*   **Stammdatenverwaltung:**
    *   Schüler, Klassen, Kurse, Lehrer und Fächer anlegen und verwalten.
*   **Stundenplan:**
    *   Wöchentliche Ansicht des Stundenplans.
*   **Datenerfassung im Unterricht:**
    *   Anwesenheiten (anwesend, abwesend, verspätet) erfassen.
    *   Spontannoten und Kommentare für Schüler eintragen.
*   **Leistungsverwaltung:**
    *   Leistungsabfragen (z.B. Klassenarbeiten) definieren.
    *   Notenschlüssel verwalten und anwenden.
    *   Ergebnisse via CSV importieren.
*   **Responsive Benutzeroberfläche:**
    *   Modernes, sauberes Design, das auf mobilen Geräten und Desktops funktioniert.

## Architektur & Technologie-Stack

*   **Backend:** Python 3 mit der Standardbibliothek (`http.server`, `sqlite3`).
*   **Datenbank:** SQLite (`school.db` Datei im Hauptverzeichnis).
*   **Templating:** Jinja2 zur Trennung von Logik und Darstellung.
*   **Frontend:** Semantisches HTML5 mit [Pico.css](https://picocss.com/) für responsives Styling.

## Setup & Ausführung

1.  **Abhängigkeiten installieren:**
    ```bash
    pip install Jinja2
    ```
2.  **Anwendung starten:**
    ```bash
    python3 webapp.py
    ```
3.  **Im Browser öffnen:**
    Die Anwendung ist unter [http://localhost:8000](http://localhost:8000) erreichbar.

## Projekt-Timeline

*   **Initialversion:**
    *   Grundlegende Funktionalität zur Schulverwaltung.
    *   Die Benutzeroberfläche wurde direkt im Python-Code als HTML-Strings generiert.
    *   Das Design war nicht für mobile Geräte optimiert.
*   **v2 (UI-Refactoring - September 2025):**
    *   **Neue Feature:** Komplette Überarbeitung der Benutzeroberfläche für eine moderne, responsive Darstellung.
    *   **Neue Architektur:** Einführung der Jinja2-Templating-Engine zur sauberen Trennung von Backend-Logik und Frontend-Code.
    *   **Neue Architektur:** Integration des Pico.css-Frameworks für ein leichtgewichtiges und ansprechendes Design, das auf allen Gerätegrößen funktioniert.
