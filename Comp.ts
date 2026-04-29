
Agis en tant qu'expert Angular et UI/UX. Je dois développer un composant de mapping de données JSON visuel et interactif en utilisant Angular et Angular Material (spécifiquement @angular/cdk/drag-drop et @angular/material/tree). Merci d'utiliser la syntaxe moderne d'Angular (Standalone components, Signals, control flow).
​Architecture de l'interface (Split Screen) :
​1. Panneau de gauche (Source des données) :
​Doit afficher un fichier JSON potentiellement imbriqué sous forme d'arborescence (MatTree).
​Règle UX cruciale : L'utilisateur final ne connaît pas les clés techniques du JSON, il réagit aux valeurs. Chaque nœud de l'arbre doit donc impérativement afficher la clé ET un échantillon de la valeur (ex: email: "jean.dupont@test.com" au lieu de juste email).
​Les nœuds finaux (les feuilles de l'arbre contenant les valeurs) doivent être configurés comme des éléments glissables (cdkDrag).
​2. Panneau de droite (Cible / Mapping) :
​Affiche une liste de rubriques/labels existants de mon interface.
​Ces rubriques doivent agir comme des zones de réception (cdkDropList).
​L'utilisateur doit pouvoir glisser un élément du panneau de gauche et le lâcher sur une rubrique du panneau de droite pour établir le "map".
​Fonctionnalité d'ajout : Présence d'un bouton "Ajouter une nouvelle rubrique" permettant à l'utilisateur de créer dynamiquement un nouveau label cible (input textuel) si la donnée du JSON ne correspond à rien d'existant, puis d'y glisser sa donnée.
​Livrables attendus :
​Le code du composant (TypeScript avec Signals pour gérer l'état du mapping et de l'arbre, HTML, et SCSS avec Flexbox/Grid pour la mise en page).
​L'implémentation complète du Drag & Drop CDK entre le MatTree et la liste.
​Une structure de données finale qui exporte le résultat du mapping sous forme d'objet clé/valeur.
​Un petit jeu de données mockées (JSON source et rubriques cibles initiales) pour que je puisse tester le rendu immédiatement.

import { Component, signal, computed } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatTabsModule } from '@angular/material/tabs';
import { MatCheckboxModule } from '@angular/material/checkbox';
import { IFilterAngularComp } from 'ag-grid-angular';
import { IFilterParams, IDoesFilterPassParams } from 'ag-grid-community';

@Component({
  selector: 'app-tabbed-set-filter',
  standalone: true,
  imports: [FormsModule, MatTabsModule, MatCheckboxModule],
  template: `
    <div class="mat-filter-container">
      <mat-tab-group>
        
        <mat-tab label="Valeurs Type 1">
          <div class="checkbox-list">
            @for (val of valuesTab1(); track val) {
              <mat-checkbox 
                [checked]="selectedValuesTab1().has(val)"
                (change)="toggleSelectionTab1(val)">
                {{ val }}
              </mat-checkbox>
            }
            @if (valuesTab1().length === 0) {
              <p class="empty-msg">Aucune valeur</p>
            }
          </div>
        </mat-tab>

        <mat-tab label="Valeurs Type 2">
          <div class="checkbox-list">
            @for (val of valuesTab2(); track val) {
              <mat-checkbox 
                [checked]="selectedValuesTab2().has(val)"
                (change)="toggleSelectionTab2(val)">
                {{ val }}
              </mat-checkbox>
            }
            @if (valuesTab2().length === 0) {
              <p class="empty-msg">Aucune valeur</p>
            }
          </div>
        </mat-tab>

      </mat-tab-group>
    </div>
  `,
  styles: [`
    .mat-filter-container { width: 300px; background: white; max-height: 400px; display: flex; flex-direction: column; }
    .checkbox-list { padding: 16px; display: flex; flex-direction: column; gap: 8px; max-height: 250px; overflow-y: auto; }
    .empty-msg { color: gray; font-size: 12px; font-style: italic; }
    :host { display: block; margin: -4px; }
  `]
})
export class TabbedSetFilterComponent implements IFilterAngularComp {
  private params!: IFilterParams;

  // Les listes de valeurs disponibles pour chaque onglet
  valuesTab1 = signal<string[]>([]);
  valuesTab2 = signal<string[]>([]);

  // L'état des cases cochées (on utilise un Set pour des recherches rapides)
  selectedValuesTab1 = signal<Set<string>>(new Set());
  selectedValuesTab2 = signal<Set<string>>(new Set());

  agInit(params: IFilterParams): void {
    this.params = params;
    this.extractUniqueValues();
  }

  // Méthode pour peupler tes listes de valeurs à partir des données de la grille
  private extractUniqueValues() {
    const tab1Set = new Set<string>();
    const tab2Set = new Set<string>();

    this.params.api.forEachLeafNode(node => {
      // Adapter ici selon la structure de tes données. 
      // Par exemple, si ta ligne contient { type1: 'A', type2: 'B' }
      const data = node.data;
      if (data) {
        if (data.valeurType1) tab1Set.add(data.valeurType1);
        if (data.valeurType2) tab2Set.add(data.valeurType2);
      }
    });

    // On convertit les Set en tableaux triés pour l'affichage
    this.valuesTab1.set(Array.from(tab1Set).sort());
    this.valuesTab2.set(Array.from(tab2Set).sort());
  }

  toggleSelectionTab1(val: string) {
    const currentSet = new Set(this.selectedValuesTab1());
    if (currentSet.has(val)) {
      currentSet.delete(val);
    } else {
      currentSet.add(val);
    }
    this.selectedValuesTab1.set(currentSet);
    this.params.filterChangedCallback();
  }

  toggleSelectionTab2(val: string) {
    const currentSet = new Set(this.selectedValuesTab2());
    if (currentSet.has(val)) {
      currentSet.delete(val);
    } else {
      currentSet.add(val);
    }
    this.selectedValuesTab2.set(currentSet);
    this.params.filterChangedCallback();
  }

  isFilterActive(): boolean {
    // Le filtre est actif si au moins une case est cochée dans l'un des onglets
    return this.selectedValuesTab1().size > 0 || this.selectedValuesTab2().size > 0;
  }

  doesFilterPass(params: IDoesFilterPassParams): boolean {
    const data = params.node.data;
    if (!data) return false;

    // Logique de filtrage. Si des cases sont cochées dans l'onglet 1, la ligne doit correspondre.
    const passTab1 = this.selectedValuesTab1().size === 0 || this.selectedValuesTab1().has(data.valeurType1);
    const passTab2 = this.selectedValuesTab2().size === 0 || this.selectedValuesTab2().has(data.valeurType2);

    // Ajuste avec un || (OU) ou un && (ET) selon ton besoin métier
    return passTab1 && passTab2; 
  }

  getModel() {
    if (!this.isFilterActive()) return null;
    return {
      tab1: Array.from(this.selectedValuesTab1()),
      tab2: Array.from(this.selectedValuesTab2())
    };
  }

  setModel(model: any) {
    this.selectedValuesTab1.set(new Set(model?.tab1 || []));
    this.selectedValuesTab2.set(new Set(model?.tab2 || []));
  }
}
