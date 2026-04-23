import { Component, signal } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MatTabsModule } from '@angular/material/tabs';
import { MatInputModule } from '@angular/material/input';
import { MatFormFieldModule } from '@angular/material/form-field';
import { IFilterAngularComp } from 'ag-grid-angular';
import { IFilterParams, IDoesFilterPassParams } from 'ag-grid-community';

@Component({
  selector: 'app-material-tabbed-filter',
  standalone: true,
  imports: [FormsModule, MatTabsModule, MatInputModule, MatFormFieldModule],
  template: `
    <div class="mat-filter-container">
      <mat-tab-group>
        
        <mat-tab label="Valeur 1">
          <div class="tab-content">
            <mat-form-field appearance="outline">
              <mat-label>Filtre principal</mat-label>
              <input matInput 
                     [ngModel]="filterValue1()" 
                     (ngModelChange)="onFilter1Change($event)" 
                     placeholder="Saisissez une valeur..." />
            </mat-form-field>
          </div>
        </mat-tab>

        <mat-tab label="Valeur 2">
          <div class="tab-content">
            <mat-form-field appearance="outline">
              <mat-label>Filtre secondaire</mat-label>
              <input matInput 
                     [ngModel]="filterValue2()" 
                     (ngModelChange)="onFilter2Change($event)" 
                     placeholder="Saisissez une valeur..." />
            </mat-form-field>
          </div>
        </mat-tab>

      </mat-tab-group>
    </div>
  `,
  styles: [`
    /* On ajuste la largeur pour que les onglets Material s'affichent correctement */
    .mat-filter-container { width: 300px; background: white; }
    .tab-content { padding: 16px; display: flex; flex-direction: column; }
    mat-form-field { width: 100%; }
    
    /* Optionnel : Supprimer le padding par défaut d'AG Grid autour du filtre */
    :host { display: block; margin: -4px; }
  `]
})
export class MaterialTabbedFilterComponent implements IFilterAngularComp {
  private params!: IFilterParams;
  
  // Utilisation des Signals d'Angular 20 pour un état prévisible et performant
  filterValue1 = signal('');
  filterValue2 = signal('');

  agInit(params: IFilterParams): void {
    this.params = params;
  }

  isFilterActive(): boolean {
    return this.filterValue1().trim() !== '' || this.filterValue2().trim() !== '';
  }

  doesFilterPass(params: IDoesFilterPassParams): boolean {
    const cellValue = this.params.getValue(params.node);
    if (cellValue == null) return false;

    const cellValueLower = cellValue.toString().toLowerCase();
    
    // Lecture des valeurs via l'appel des signals
    const val1 = this.filterValue1().toLowerCase();
    const val2 = this.filterValue2().toLowerCase();
    
    const pass1 = val1 ? cellValueLower.includes(val1) : false;
    const pass2 = val2 ? cellValueLower.includes(val2) : false;

    return pass1 || pass2;
  }

  getModel() {
    if (!this.isFilterActive()) return null;
    return { value1: this.filterValue1(), value2: this.filterValue2() };
  }

  setModel(model: any) {
    // Mise à jour des signals lors de la restauration de l'état
    this.filterValue1.set(model?.value1 || '');
    this.filterValue2.set(model?.value2 || '');
  }

  onFilter1Change(newValue: string) {
    this.filterValue1.set(newValue);
    this.params.filterChangedCallback(); // Notifie AG Grid de relancer le filtrage
  }

  onFilter2Change(newValue: string) {
    this.filterValue2.set(newValue);
    this.params.filterChangedCallback();
  }
}
